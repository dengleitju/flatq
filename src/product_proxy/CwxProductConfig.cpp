#include "CwxProductConfig.h"

bool CwxProductConfig::fetchHost(CwxIniParse& cnf, string const& node,
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


int CwxProductConfig::loadConfig(string const& strConfFile) {
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

  ///load cmn:send_timeout
  if (!cnf.getAttr("cmn", "send_timeout", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:send_timeout].");
    return -1;
  }
  m_common.m_uiSendTimeout = strtoul(value.c_str(), NULL, 10);
  if (m_common.m_uiSendTimeout < MIN_SEND_TIMEOUT) {
    m_common.m_uiSendTimeout = MIN_SEND_TIMEOUT;
  }
  if (m_common.m_uiSendTimeout > MAX_SEND_TIMEOUT) {
    m_common.m_uiSendTimeout = MAX_SEND_TIMEOUT;
  }

  ///load cmn:max_queue_num
  if (!cnf.getAttr("cmn", "max_queue_num", value) || !value.length()) {
    snprintf(m_szErrMsg, 2047, "Must set [cmn:max_queue_num]");
    return -1;
  }
  m_common.m_uiMaxQueueNum = strtoul(value.c_str(), NULL, 10);
  if (m_common.m_uiMaxQueueNum > CwxProductConfigCmn::MAX_QUEUE_NUM) {
    m_common.m_uiMaxQueueNum = CwxProductConfigCmn::MAX_QUEUE_NUM;
  }
  if (m_common.m_uiMaxQueueNum < CwxProductConfigCmn::MIN_QUEUE_NUM) {
    m_common.m_uiMaxQueueNum = CwxProductConfigCmn::MIN_QUEUE_NUM;
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

void CwxProductConfig::outputConfig() const {
  CWX_INFO(("******************Product-proxy CONFIG***********************"));
  CWX_INFO(("*********************common**********************************"));
  CWX_INFO(("WorkDir:%s", m_common.m_strWorkDir.c_str()));
  CWX_INFO(("SocketBufSize:%u", m_common.m_uiSockBufSize));
  CWX_INFO(("SendTimeout:%u", m_common.m_uiSendTimeout));
  CWX_INFO(("MaxQueueNum:%u", m_common.m_uiMaxQueueNum));
  CWX_INFO(("*********************Zookeeper*******************************"));
  CWX_INFO(("Root:%s", m_zk.m_strRootPath.c_str()));
  CWX_INFO(("ZkServer:%s", m_zk.m_strZkServer.c_str()));
  CWX_INFO(("ZkAuth:%s", m_zk.m_strAuth.c_str()));
}
