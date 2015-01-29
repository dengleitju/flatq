#include "CwxSubProxyConfig.h"

bool CwxSubConfig::fetchHost(CwxIniParse& cnf, string const& node,
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


int CwxSubConfig::loadConfig(string const& strConfFile) {
  CwxIniParse cnf;
  string value;
  string strErrMsg;
  ///解析配置文件
  if (false == cnf.load(strConfFile)) {
    CwxCommon::snprintf(m_szErrMsg, 2047,
        "Failure to load conf file:%s, err:%s", strConfFile.c_str(),
        cnf.getErrMsg());
    return -1;
  }

  ///load cmn:home
  if (!cnf.getAttr("cmn", "home", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:home]");
    return -1;
  }
  if ('/' != value[value.length()-1])
    value += "/";
  m_common.m_strWorkDir = value;

  //load cmn:sock_buf_kbyte
  if (!cnf.getAttr("cmn", "sock_buf_kbyte", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:sock_buf_kbyte].");
    return -1;
  }
  m_common.m_uiSockBufSize = strtoul(value.c_str(), NULL, 10);
  if (m_common.m_uiSockBufSize < MIN_SOCK_BUF_KB) {
    m_common.m_uiSockBufSize = MIN_SOCK_BUF_KB;
  }
  if (m_common.m_uiSockBufSize > MAX_SOCK_BUF_KB) {
    m_common.m_uiSockBufSize = MAX_SOCK_BUF_KB;
  }

  ///load cmn:m_uiSyncNum
  if (!cnf.getAttr("cmn", "sync_conn_num", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:sync_num].");
    return -1;
  }
  m_common.m_uiSyncNum = strtoul(value.c_str(), NULL, 10);
  if (m_common.m_uiSyncNum < MIN_SYNC_NUM) {
    m_common.m_uiSyncNum = MIN_SYNC_NUM;
  }
  if (m_common.m_uiSyncNum > MAX_SYNC_NUM) {
    m_common.m_uiSyncNum = MAX_SYNC_NUM;
  }

  //load sync:max_chunk_kbyte
  if (!cnf.getAttr("cmn", "max_chunk_kbyte", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:max_chunk_kbyte].");
    return -1;
  }
  m_common.m_uiChunkKBye = strtoul(value.c_str(), NULL, 10);
  if (m_common.m_uiChunkKBye > CWX_MQ_MAX_CHUNK_KSIZE)
    m_common.m_uiChunkKBye = CWX_MQ_MAX_CHUNK_KSIZE;

  //load sync:zip
  if (!cnf.getAttr("cmn", "zip", value) || !value.length()) {
    m_common.m_bzip = false;
  } else {
    if (value == "yes")
      m_common.m_bzip = true;
    else
      m_common.m_bzip = false;
  }


  ///load cmn:listen
  if (!cnf.getAttr("cmn", "listen", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:listen].");
    return -1;
  }
  if (!mqParseHostPort(value, m_common.m_listen)) {
    snprintf(m_szErrMsg, 2047, "cmn:listen must be [host:port], [%s] is invalid.",
        value.c_str());
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
  if ('/' == value[value.length() - 1])
    value = value.substr(0, value.length()-2);
  m_zk.m_strRootPath = value;

  return 0;
}

void CwxSubConfig::outputConfig() const {
  CWX_INFO(("******************Product-proxy CONFIG***********************"));
  CWX_INFO(("*********************common**********************************"));
  CWX_INFO(("WorkDir:%s", m_common.m_strWorkDir.c_str()));
  CWX_INFO(("SocketBufSize:%u", m_common.m_uiSockBufSize));
  CWX_INFO(("SyncNum:%u", m_common.m_uiSyncNum));
  CWX_INFO(("*********************Zookeeper*******************************"));
  CWX_INFO(("Root:%s", m_zk.m_strRootPath.c_str()));
  CWX_INFO(("ZkServer:%s", m_zk.m_strZkServer.c_str()));
  CWX_INFO(("ZkAuth:%s", m_zk.m_strAuth.c_str()));
}
