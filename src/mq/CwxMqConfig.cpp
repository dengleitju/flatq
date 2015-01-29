#include "CwxMqConfig.h"
#include "CwxMqApp.h"


int CwxMqConfig::loadTopic(string const& strTopicFile) {
  m_topics.clear();
  CwxIniParse cnf;
  string value;
  string strErrMsg;
  ///解析配置文件
  if (false == cnf.load(strTopicFile)) {
    CwxCommon::snprintf(m_szErrMsg, 2047,
        "Failure to Load conf file:%s. err:%s", strTopicFile.c_str(),
        cnf.getErrMsg());
    return -1;
  }
  //load topic
  list<pair<string, string> > topics;
  if (!cnf.getAttr("topic", topics) || !topics.size()) {
    snprintf(m_szErrMsg, 2047, "Must set [topic].");
    return -1;
  }
  list<pair<string, string> >::iterator iter = topics.begin();
  while (iter != topics.end()) {
    int state = strtoul(iter->second.c_str(), NULL, 10);
    if (state != CWX_MQ_TOPIC_NORMAL &&
        state != CWX_MQ_TOPIC_FREEZE &&
        state != CWX_MQ_TOPIC_DELETE) {
      snprintf(m_szErrMsg, 2047, "Topic:%s state:%s invalid.", iter->first.c_str(), iter->second.c_str());
      return -1;
    }
    m_topics[iter->first] = state;
    iter++;
  }
  return 0;
}

///保存topic/state信息
int CwxMqConfig::saveTopic(string const& strTopicFile, map<string, CWX_UINT8>& topics) {
  string strNewFileName = strTopicFile + ".new";
  int fd = ::open(strNewFileName.c_str(),  O_RDWR | O_CREAT | O_TRUNC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (-1 == fd) {
    CwxCommon::snprintf(m_szErrMsg, 2047,
        "Failure to open new topic-state file:%s, errno=%d", strNewFileName.c_str(), errno);
    ::close(fd);
    return -1;
  }
  //写入topic
  char line[1024];
  ssize_t len = 0;
  len = CwxCommon::snprintf(line, 2047, "[topic]\r\n");
  ///[topic]
  if (len != write(fd, line, len)) {
    ::close(fd);
    CwxCommon::snprintf(m_szErrMsg, 2047,
        "Failure to write new topic-state file:%s, errno=%d", strNewFileName.c_str(), errno);
    return -1;
  }
  for(map<string, CWX_UINT8>::iterator iter = topics.begin(); iter != topics.end(); iter++) {
    len = CwxCommon::snprintf(line, 1024, "%s=%d\r\n", iter->first.c_str(), iter->second);
    if (len != write(fd, line, len)) {
      ::close(fd);
      CwxCommon::snprintf(m_szErrMsg, 2047,
          "Failure to write new topic-state file:%s, errno=%d", strNewFileName.c_str(), errno);
      return -1;
    }
  }
  ::close(fd);
  CwxFile::moveFile(strNewFileName.c_str(), strTopicFile.c_str());
  return 0;
}

int CwxMqConfig::loadConfig(string const & strConfFile) {
  CwxIniParse cnf;
  string value;
  string strErrMsg;
  //解析配置文件
  if (false == cnf.load(strConfFile)) {
    CwxCommon::snprintf(m_szErrMsg, 2047,
      "Failure to Load conf file:%s. err:%s", strConfFile.c_str(),
      cnf.getErrMsg());
    return -1;
  }

  //load cmn:home
  if (!cnf.getAttr("cmn", "home", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:home].");
    return -1;
  }
  if ('/' != value[value.length() - 1])
    value += "/";
  m_common.m_strWorkDir = value;

  //load  cmn:server_type
  if (!cnf.getAttr("cmn", "server_type", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:server_type].");
    return -1;
  }
  if (value == "master") {
    m_common.m_type = CwxMqConfigCmn::MQ_TYPE_MASTER;
  } else if (value == "slave") {
    m_common.m_type = CwxMqConfigCmn::MQ_TYPE_SLAVE;
  } else if (value == "zk") {
    m_common.m_type = CwxMqConfigCmn::MQ_TYPE_ZK;
  } else {
    CwxCommon::snprintf(m_szErrMsg, 2047,
      "[cmn:server_type] must be [master] or [slave] or [zk].");
    return -1;
  }
  
  if (m_common.m_type != CwxMqConfigCmn::MQ_TYPE_ZK) {
    ///load cmn:topic
    if (!cnf.getAttr("cmn", "topic", value) || !value.length()) {
      snprintf(m_szErrMsg, 2047, "Must set [cmn:topic].");
      return -1;
    }
    m_common.m_strTopicConf = value;
    if ((m_common.m_type == CwxMqConfigCmn::MQ_TYPE_MASTER) &&
        (0!=loadTopic(value))) return -1;
  }

  //load cmn:debug
  if (cnf.getAttr("cmn", "debug", value) && value.length()) {
    m_common.m_bDebug = (value=="yes"?true:false);
  }

  //load cmn:sock_buf_kbyte
  if (!cnf.getAttr("cmn", "sock_buf_kbyte", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:sock_buf_kbyte].");
    return -1;
  }
  m_common.m_uiSockBufSize = strtoul(value.c_str(), NULL, 10);
  if (m_common.m_uiSockBufSize < CwxMqConfigCmn::MIN_SOCK_BUF_KB) {
    m_common.m_uiSockBufSize = CwxMqConfigCmn::MIN_SOCK_BUF_KB;
  }
  if (m_common.m_uiSockBufSize > CwxMqConfigCmn::MAX_SOCK_BUF_KB) {
    m_common.m_uiSockBufSize = CwxMqConfigCmn::MAX_SOCK_BUF_KB;
  }
  //load cmn:max_chunk_kbyte
  if (!cnf.getAttr("cmn", "max_chunk_kbyte", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:max_chunk_kbyte].");
    return -1;
  }
  m_common.m_uiChunkSize = strtoul(value.c_str(), NULL, 10);
  if (m_common.m_uiChunkSize < CwxMqConfigCmn::MIN_CHUNK_SIZE_KB) {
    m_common.m_uiChunkSize = CwxMqConfigCmn::MIN_CHUNK_SIZE_KB;
  }
  if (m_common.m_uiChunkSize > CwxMqConfigCmn::MAX_CHUNK_SIZE_KB) {
    m_common.m_uiChunkSize = CwxMqConfigCmn::MAX_CHUNK_SIZE_KB;
  }
  //load cmn:sync_conn_num
  if (!cnf.getAttr("cmn", "sync_conn_num", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:sync_conn_num].");
    return -1;
  }
  m_common.m_uiSyncConnNum = strtoul(value.c_str(), NULL, 10);
  if (m_common.m_uiSyncConnNum < CwxMqConfigCmn::MIN_SYNC_CONN_NUM) {
    m_common.m_uiSyncConnNum = CwxMqConfigCmn::MIN_SYNC_CONN_NUM;
  }
  if (m_common.m_uiSyncConnNum > CwxMqConfigCmn::MAX_SYNC_CONN_NUM) {
    m_common.m_uiSyncConnNum = CwxMqConfigCmn::MAX_SYNC_CONN_NUM;
  }
  //load cmn:monitor
  if (!cnf.getAttr("cmn", "monitor", value) || !value.length()) {
    m_common.m_monitor.reset();
  } else {
    if (!mqParseHostPort(value, m_common.m_monitor)) {
      snprintf(m_szErrMsg, 2047,
        "cmn:monitor must be [host:port], [%s] is invalid.", value.c_str());
      return -1;
    }
  }
  //load cmn:mq_id
  if (!cnf.getAttr("cmn", "mq_id", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:mq_id].");
    return -1;
  }
  m_common.m_strMqId = value;

  //load binlog:path
  if (!cnf.getAttr("binlog", "path", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [binlog:path].");
    return -1;
  }
  if ('/' != value[value.length() - 1])
    value += "/";
  m_binlog.m_strBinlogPath = value;

  //load binlog:file_prefix
  if (!cnf.getAttr("binlog", "file_prefix", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [binlog:file_prefix].");
    return -1;
  }
  m_binlog.m_strBinlogPrex = value;

  //load binlog:file_max_mbyte
  if (!cnf.getAttr("binlog", "file_max_mbyte", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [binlog:file_max_mbyte].");
    return -1;
  }
  m_binlog.m_uiBinLogMSize = strtoul(value.c_str(), NULL, 10);
  if (m_binlog.m_uiBinLogMSize < CwxMqConfigBinLog::MIN_BINLOG_MSIZE) {
    m_binlog.m_uiBinLogMSize = CwxMqConfigBinLog::MIN_BINLOG_MSIZE;
  }
  if (m_binlog.m_uiBinLogMSize > CwxMqConfigBinLog::MAX_BINLOG_MSIZE) {
    m_binlog.m_uiBinLogMSize = CwxMqConfigBinLog::MAX_BINLOG_MSIZE;
  }
  m_binlog.m_uiBinLogMSize *= 1024 * 1024;
  //load binlog:max_file_num
  if (!cnf.getAttr("binlog", "max_file_num", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [binlog:max_file_num].");
    return -1;
  }
  m_binlog.m_uiMgrFileNum = strtoul(value.c_str(), NULL, 10);
  if (m_binlog.m_uiMgrFileNum < CwxBinLogMgr::MIN_MANAGE_FILE_NUM) {
    m_binlog.m_uiMgrFileNum = CwxBinLogMgr::MIN_MANAGE_FILE_NUM;
  }
  if (m_binlog.m_uiMgrFileNum > CwxBinLogMgr::MAX_MANAGE_FILE_NUM) {
    m_binlog.m_uiMgrFileNum = CwxBinLogMgr::MAX_MANAGE_FILE_NUM;
  }
  //load binlog:del_out_file
  /*if (!cnf.getAttr("binlog", "del_out_file", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [binlog:del_out_file].");
    return -1;
  }*/
  m_binlog.m_bDelOutdayLogFile =true; //// (value == "yes" ? true : false);

  //load binlog:flush_log_num
  if (!cnf.getAttr("binlog", "flush_log_num", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [binlog:flush_log_num].");
    return -1;
  }
  m_binlog.m_uiFlushNum = strtoul(value.c_str(), NULL, 10);
  if (m_binlog.m_uiFlushNum < 1) {
    m_binlog.m_uiFlushNum = 1;
  }
  if (m_binlog.m_uiFlushNum > CWX_MQ_MAX_BINLOG_FLUSH_COUNT) {
    m_binlog.m_uiFlushNum = CWX_MQ_MAX_BINLOG_FLUSH_COUNT;
  }
  //load binlog:flush_log_second
  if (!cnf.getAttr("binlog", "flush_log_second", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [binlog:flush_log_second].");
    return -1;
  }
  m_binlog.m_uiFlushSecond = strtoul(value.c_str(), NULL, 10);
  if (m_binlog.m_uiFlushSecond < 1) {
    m_binlog.m_uiFlushSecond = 1;
  }
  //load binlog:is_merge
  if (cnf.getAttr("binlog", "is_merge", value) && value.length()) {
    m_binlog.m_bIsMerge = (value == "yes" ? true : false);
  }
  //load binlog:merge_num
  if (cnf.getAttr("binlog", "merge_size", value) && value.length()) {
    m_binlog.m_uiMergeSize = strtoul(value.c_str(), NULL, 10) * 1024;
  }
  if (m_binlog.m_uiMergeSize > 1024 * 1024) {
    m_binlog.m_uiMergeSize = 1024 * 1024;
  }
  //load binlog:save_file_day
  m_binlog.m_uiSaveFileDay = CwxMqConfigBinLog::DEF_SAVE_BINLOG_DAY;
  if (cnf.getAttr("binlog", "save_file_day", value) && value.length()) {
    m_binlog.m_uiSaveFileDay = strtoul(value.c_str(), NULL, 10);
  }
  if (m_binlog.m_uiSaveFileDay > CwxMqConfigBinLog::MAX_SAVE_BINLOG_DAY)
    m_binlog.m_uiSaveFileDay = CwxMqConfigBinLog::MAX_SAVE_BINLOG_DAY;
  if (m_binlog.m_uiSaveFileDay < CwxMqConfigBinLog::MIN_SAVE_BINLOG_DAY)
    m_binlog.m_uiSaveFileDay = CwxMqConfigBinLog::MIN_SAVE_BINLOG_DAY;

  //load inner dispatch
  if (cnf.isExistSection("inner_dispatch")) {
    //load dispatch的监听
    if (!fetchHost(cnf, "inner_dispatch", m_innerDispatch.m_async))
      return -1;
  }

  //load outer dispatch
  if (cnf.isExistSection("outer_dispatch")) {
    //load dispatch的监听
    if (!fetchHost(cnf, "outer_dispatch", m_outerDispatch.m_async))
      return -1;
    //load dispatch:source_flush_num
    if (!cnf.getAttr("outer_dispatch", "source_flush_num", value)
      || !value.length()) {
        snprintf(m_szErrMsg, 2047, "Must set [outer_dispatch:source_flush_num].");
        return -1;
    }
    m_outerDispatch.m_uiFlushNum = strtoul(value.c_str(), NULL, 10);
    if (m_outerDispatch.m_uiFlushNum < 1) {
      m_outerDispatch.m_uiFlushNum = 1;
    }
    //load dispatch:source_flush_second
    if (!cnf.getAttr("outer_dispatch", "source_flush_second", value)
      || !value.length()) {
        snprintf(m_szErrMsg, 2047, "Must set [outer_dispatch:source_flush_second].");
        return -1;
    }
    m_outerDispatch.m_uiFlushSecond = strtoul(value.c_str(), NULL, 10);
    if (m_outerDispatch.m_uiFlushSecond < 1) {
      m_outerDispatch.m_uiFlushSecond = 1;
    }
    ///load dispatch:source_path
    if (!cnf.getAttr("outer_dispatch", "source_path", value) || !value.length()) {
      snprintf(m_szErrMsg, 2047, "Must set [outer_dispatch:source_path].");
      return -1;
    }
    if ('/' != value[value.length() - 1])
      value += "/";
    m_outerDispatch.m_strSourcePath = value;
  }


  if (m_common.m_type == CwxMqConfigCmn::MQ_TYPE_MASTER) {
    //load recv
    if (!fetchHost(cnf, "recv", m_recv.m_recv))
      return -1;
  } else if(m_common.m_type == CwxMqConfigCmn::MQ_TYPE_SLAVE) {
    //load master
    if (!fetchHost(cnf, "master", m_master.m_master))
      return -1;
    //load master:zip
    if (!cnf.getAttr("master", "zip", value) || !value.length()) {
      m_master.m_bzip = false;
    } else {
      if (value == "yes")
        m_master.m_bzip = true;
      else
        m_master.m_bzip = false;
    }
  }else if (m_common.m_type == CwxMqConfigCmn::MQ_TYPE_ZK) {
    //load recv
    if (!fetchHost(cnf, "recv", m_recv.m_recv)) {
      return -1;
    }
    // load zk
    //get zk:server
    if (!cnf.getAttr("zk", "server", value) || !value.length()) {
      snprintf(m_szErrMsg, 2047, "Must set [zk:server]");
      return -1;
    }
    m_zk.m_strZkServer = value;
    //get zk:auth
    if (cnf.getAttr("zk", "auth", value)) {
      m_zk.m_strAuth = value;
    }
    //get zk:root
    if (!cnf.getAttr("zk", "root", value) || !value.length()) {
      snprintf(m_szErrMsg, 2047, "Must set [zk:root].");
      return -1;
    }
    if ('/' != value[value.length() - 1])
      value += "/";
    m_zk.m_strRootPath = value;
    //get zk:group
    if (!cnf.getAttr("zk", "group", value) || !value.length()) {
      snprintf(m_szErrMsg, 2047, "Must set [zk:group].");
      return -1;
    }
    m_zk.m_strGroup = value;
  }

  return 0;
}

bool CwxMqConfig::fetchHost(CwxIniParse& cnf, string const& node,
                            CwxHostInfo& host)
{
  string value;
  host.reset();
  //get listen
  if (cnf.getAttr(node, "listen", value) && value.length()) {
    if (!mqParseHostPort(value, host)) {
      snprintf(m_szErrMsg, 2047,
        "%s:listen must be [host:port], [%s] is invalid.", node.c_str(),
        value.c_str());
      return false;
    }
  }
  //load keepalive
  if (cnf.getAttr(node, "keepalive", value) && value.length()) {
    host.setKeepAlive(value == "yes" ? true : false);
  } else {
    host.setKeepAlive(false);
  }
  if (!host.getHostName().length()) {
    CwxCommon::snprintf(m_szErrMsg, 2047, "Must set [%s]'s [listen].",
      node.c_str());
    return false;
  }
  return true;
}

void CwxMqConfig::outputConfig() const {
  CWX_INFO(("\n*****************BEGIN CONFIG*******************"));
  CWX_INFO(("*****************cmn*******************"));
  CWX_INFO(("home=%s", m_common.m_strWorkDir.c_str()));
  if (m_common.m_type == CwxMqConfigCmn::MQ_TYPE_MASTER) {
    CWX_INFO(("server_type=master"));
  }else if (m_common.m_type == CwxMqConfigCmn::MQ_TYPE_SLAVE) {
    CWX_INFO(("server_type=slave"));
  }else if (m_common.m_type == CwxMqConfigCmn::MQ_TYPE_ZK) {
    CWX_INFO(("server_type=zk"));
  }else {
    CWX_INFO(("server_type="));
  }
  CWX_INFO(("sock_buf_kbyte=%u", m_common.m_uiSockBufSize));
  CWX_INFO(("max_chunk_kbyte=%u", m_common.m_uiChunkSize));
  CWX_INFO(("sync_conn_num=%u", m_common.m_uiSyncConnNum));
  CWX_INFO(("monitor=%s:%u", m_common.m_monitor.getHostName().c_str(), m_common.m_monitor.getPort()));
  CWX_INFO(("debug=%s", m_common.m_bDebug?"yes":"no"));
  CWX_INFO(("mq_id=%s", m_common.m_strMqId.c_str()));
  CWX_INFO(("*****************binlog*******************"));
  CWX_INFO(("path=%s", m_binlog.m_strBinlogPath.c_str()));
  CWX_INFO(("file_prefix=%s", m_binlog.m_strBinlogPrex.c_str()));
  CWX_INFO(("file_max_mbyte=%u", m_binlog.m_uiBinLogMSize));
  CWX_INFO(("max_file_num=%u", m_binlog.m_uiMgrFileNum));
  CWX_INFO(("del_out_file=%s", m_binlog.m_bDelOutdayLogFile?"yes":"no"));
  CWX_INFO(("flush_log_num=%u", m_binlog.m_uiFlushNum));
  CWX_INFO(("flush_log_second=%u", m_binlog.m_uiFlushSecond));
  CWX_INFO(("is_merge=%s", m_binlog.m_bIsMerge?"yes":"no"));
  CWX_INFO(("merge_size=%u",m_binlog.m_uiMergeSize));
  CWX_INFO(("save_file_day=%u", m_binlog.m_uiSaveFileDay));
  CWX_INFO(("*****************inner dispatch*******************"));
  CWX_INFO(
    ("listen=%s:%u", m_innerDispatch.m_async.getHostName().c_str(), m_innerDispatch.m_async.getPort()));
  CWX_INFO(("*****************outer dispatch*******************"));
  CWX_INFO(
    ("listen=%s:%u", m_outerDispatch.m_async.getHostName().c_str(), m_outerDispatch.m_async.getPort()));
  CWX_INFO(("source_path=%s", m_outerDispatch.m_strSourcePath.c_str()));
  CWX_INFO(("source_flush_num=%u", m_outerDispatch.m_uiFlushNum));
  CWX_INFO(("source_flush_second=%u", m_outerDispatch.m_uiFlushSecond));
  if (m_common.m_type == CwxMqConfigCmn::MQ_TYPE_MASTER) {
    CWX_INFO(("*****************recv*******************"));
    CWX_INFO(("listen=%s:%u", m_recv.m_recv.getHostName().c_str(), m_recv.m_recv.getPort()));
    CWX_INFO(("topic file=%s", m_common.m_strTopicConf.c_str()));
  } else if (m_common.m_type == CwxMqConfigCmn::MQ_TYPE_SLAVE) {
    CWX_INFO(("*****************master*******************"));
    CWX_INFO(("listen=%s:%u", m_master.m_master.getHostName().c_str(), m_master.m_master.getPort()));
    CWX_INFO(("zip=%s", m_master.m_bzip?"yes":"no"));
  }else if (m_common.m_type == CwxMqConfigCmn::MQ_TYPE_ZK) {
    CWX_INFO(("*****************zk***********************"));
    CWX_INFO(("listen=%s:%u", m_recv.m_recv.getHostName().c_str(), m_recv.m_recv.getPort()));
    CWX_INFO(("server=%s", m_zk.m_strZkServer.c_str()));
    CWX_INFO(("auth=%s", m_zk.m_strAuth.c_str()));
    CWX_INFO(("root=%s", m_zk.m_strRootPath.c_str()));
    CWX_INFO(("group=%s", m_zk.m_strGroup.c_str()));
  }
  CWX_INFO(("*****************END   CONFIG *******************"));
}
