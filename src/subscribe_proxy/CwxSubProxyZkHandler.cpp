#include "CwxSubProxyZkHandler.h"
#include "CwxSubProxyApp.h"

CwxSubProxyZkHandler::CwxSubProxyZkHandler(CwxSubProxyApp* app,
    string const& zkRoot,
    string const & zkServer):m_pApp(app) {
  m_zk = NULL;
  m_clientId = NULL;
  m_bInit = false;
  m_bConnected = false;
  m_bAuth = false;
  m_bValid =false;
  m_strZkRoot = zkRoot;
  m_strZkServer = zkServer;
  m_ullVersion = 0;
  m_szErr2K[0] = 0x00;
}

CwxSubProxyZkHandler::~CwxSubProxyZkHandler() {
  stop();
  if (m_clientId) {
    delete m_clientId;
    m_clientId = NULL;
  }
}

void CwxSubProxyZkHandler::stop() {
  CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
  if (m_zk) {
    m_zk->disconnect();
    delete m_zk;
    m_zk = NULL;
  }
}

int CwxSubProxyZkHandler::init() {
  CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
  return _init();
}

int CwxSubProxyZkHandler::_init() {
  _reset();
  m_zk = new ZkAdaptor(m_strZkServer);
  if (0 != m_zk->init()) {
    CwxCommon::snprintf(m_szErr2K, 2048, "Failure to invoke ZkAdaptor::init(), err:%s",
        m_zk->getErrMsg());
    CWX_ERROR((m_szErr2K));
    return -1;
  }
  if (0 != _connect()) return -1;
  m_bInit = true;
  return 0;
}

void CwxSubProxyZkHandler::_reset() {
  m_bInit = false;
  m_bConnected = false;
  m_bAuth = false;
  m_bValid = false;
  strcpy(m_szErr2K, "No init.");
  if (m_zk) {
    m_zk->disconnect();
    delete m_zk;
    m_zk = NULL;
  }
  m_group.clear();
}

int CwxSubProxyZkHandler::_dealTimeoutEvent(CwxTss*, CwxMsgBlock*&, CWX_UINT32) {
  if (!m_bInit) {
    _init();
    noticeMasterMq();
  }
  if (m_bValid) {
    ///检测是否有未设置的mq-group节点
    if (0 != checkMqGroup()) return -1;
  }
  return 0;
}

int CwxSubProxyZkHandler::_dealConnectedEvent(CwxTss*, CwxMsgBlock*& msg, CWX_UINT32) {
  int ret=0;
  if (!m_bInit) return 0; ///如果未init,则忽略
  ///连接成功
  m_bConnected = true;
  ///保存zk的client id
  if (msg->length()) {
    if (!m_clientId) m_clientId = new clientid_t;
    memcpy(m_clientId, msg->rd_ptr(), sizeof(clientid_t));
  }else {
    if (m_clientId) delete m_clientId;
    m_clientId = NULL;
  }
  ///鉴权，若失败则返回
  ret = _auth();
  if (-1 == ret) { ///重新初始化
    return -1;
  }
  if (1 == ret) {
    if (0 != _watch()) return -1;
    m_bValid = true;
  }
  return 0;
}


void CwxSubProxyZkHandler::doEvent(CwxTss* tss, CwxMsgBlock* msg, CWX_UINT32 uiLeftEvent) {
  CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
  bool bSuccess = false;
  do {
    if (CwxEventInfo::TIMEOUT_CHECK == msg->event().getEvent()) {
      if (0 != _dealTimeoutEvent(tss, msg, uiLeftEvent)) break;
    }else if (EVENT_ZK_CONNECTED == msg->event().getEvent()) {
      if (0 != _dealConnectedEvent(tss, msg, uiLeftEvent)) break;
    }else if (EVENT_ZK_EXPIRED == msg->event().getEvent()) {
      if (m_clientId) delete m_clientId;
      m_clientId = NULL;
      m_bInit = false;
    }else if (EVENT_ZK_FAIL_AUTH ==  msg->event().getEvent()) {
      if (m_clientId) delete m_clientId;
      m_clientId = NULL;
      m_bInit = false;
    }else if (EVENT_ZK_SUCCESS_AUTH == msg->event().getEvent()) {
      if (0 != _watch()) break;
      if (m_bInit) m_bValid = true;
    }else if (EVENT_ZK_LOST_WATCH == msg->event().getEvent()) {
      CWX_INFO(("Get event_zk_lost_watch."));
      ///监控被移除，重新设置监控
      if (0 != _watch()) break;
    }else if (EVENT_ZK_GROUP_EVENT == msg->event().getEvent()) {
      ///mq-group的child发生变化
      if (0 != _dealMqGroupEvent(tss, msg, uiLeftEvent)) break;
    }else if (EVENT_ZK_MQ_EVENT == msg->event().getEvent()) {
      ///master-mq节点发生变化
      if (0 != _dealMasterMqEvent(tss, msg, uiLeftEvent)) break;
    }else{
      CWX_ERROR(("Unkown event type:%u", msg->event().getEvent()));
    }
    if (!m_zk) break;
    bSuccess = true;
  }while(0);
  if (!bSuccess) { ///重新初始化
    m_bInit = false;
  }
}

///0:连接成功；-1：失败
int CwxSubProxyZkHandler::_connect() {
  if (0 != m_zk->connect(m_clientId, 0, CwxSubProxyZkHandler::watcher, this)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "Failure to invoke ZkAdaptor::connect(), err:%s", m_zk->getErrMsg());
    CWX_ERROR((m_szErr2K));
    return -1;
  }
  return 0;
}


///1:认证成功；0：等待认证结果；-1：认证失败
int CwxSubProxyZkHandler::_auth() {
  if (!m_bInit) return 0; ///没有init,则忽略
  if (m_pApp->getConfig().getZk().m_strAuth.length()) {
    if (!m_zk->addAuth("digest",
        m_pApp->getConfig().getZk().m_strAuth.c_str(),
        m_pApp->getConfig().getZk().m_strAuth.length(),
        0,
        CwxSubProxyZkHandler::zkAuthCallback,
        this)) {
      CwxCommon::snprintf(m_szErr2K, 2047, "Failure to invoke ZkAdaptor:addAuth(), err:%s", m_zk->getErrMsg());
      CWX_ERROR((m_szErr2K));
      return -1;
    }
    ///等待鉴权
    return 0;
  }
  ///无需鉴权
  m_bAuth = true;
  return 1;
}

void CwxSubProxyZkHandler::watcher(zhandle_t* ,
      int type,
      int state,
      const char* path,
      void* context) {
  CwxSubProxyZkHandler* zkHandler = (CwxSubProxyZkHandler*)context;
  CwxMsgBlock* msg = NULL;
  if (type == ZOO_SESSION_EVENT) {
    if (state == ZOO_CONNECTED_STATE) {
      msg = CwxMsgBlockAlloc::malloc(sizeof(clientid_t));
      if (zkHandler->m_zk->getClientId()) {
        memcpy(msg->wr_ptr(), zkHandler->m_zk->getClientId(), sizeof(clientid_t));
        msg->wr_ptr(sizeof(clientid_t));
      }
      msg->event().setEvent(EVENT_ZK_CONNECTED);
      zkHandler->m_pApp->getZkThreadPool()->append(msg);
    }else if (state == ZOO_AUTH_FAILED_STATE) {
      msg = CwxMsgBlockAlloc::malloc(0);
      msg->event().setEvent(EVENT_ZK_FAIL_AUTH);
      zkHandler->m_pApp->getZkThreadPool()->append(msg);
    }else if (state == ZOO_EXPIRED_SESSION_STATE) {
      msg = CwxMsgBlockAlloc::malloc(0);
      msg->event().setEvent(EVENT_ZK_EXPIRED);
      zkHandler->m_pApp->getZkThreadPool()->append(msg);
    }else if (state == ZOO_CONNECTING_STATE) {
      CWX_INFO(("Connecting zookeeper :%s", zkHandler->m_pApp->getConfig().getZk().m_strZkServer.c_str()));
    }else if (state == ZOO_ASSOCIATING_STATE) {
      CWX_INFO(("Zookeeper associating"));
    }
  }else if (type == ZOO_CREATED_EVENT){
    CWX_INFO(("Zookeeper node is created:%s", path));
  }else if (type == ZOO_DELETED_EVENT) {
    CWX_INFO(("Zookeeper node is dropped:%s", path));
  }else if (type == ZOO_CHANGED_EVENT) {
    CWX_INFO(("Zookeeper node is changed:%s", path));
  }else if (type == ZOO_CHILD_EVENT) {
    CWX_INFO(("Zookeeper node's child is changed:%s", path));
  }else if (type == ZOO_NOTWATCHING_EVENT) {
    msg = CwxMsgBlockAlloc::malloc(0);
    msg->event().setEvent(EVENT_ZK_LOST_WATCH);
    zkHandler->m_pApp->getZkThreadPool()->append(msg);
    CWX_INFO(("Zookeeper node's watcher is lost:%s", path));
  }else{
    CWX_INFO(("Unknown event:type=%d, state=%d, path=%s", type, state, path?path:""));
  }
}

void CwxSubProxyZkHandler::zkAuthCallback(int rc,
      const void* data) {
  CwxSubProxyZkHandler* zk = (CwxSubProxyZkHandler*)data;
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
  msg->event().setSvrId(CwxSubProxyApp::SVR_SUB_TYPE_ZK);
  msg->event().setEvent(ZOK==rc?EVENT_ZK_SUCCESS_AUTH:EVENT_ZK_FAIL_AUTH);
  zk->m_pApp->getZkThreadPool()->append(msg);
}


int CwxSubProxyZkHandler::_watch() {
  if (!m_bInit) return 0; ///如果未init,则忽略
  if (!m_zk) return 0;
  m_group.clear();
  list<string> childs;
  list<string>::iterator child_iter;
  ///设置对根目录监控
  int ret = m_zk->wgetNodeChildren(m_strZkRoot.c_str(), childs,
      CwxSubProxyZkHandler::watcherMqGroup, this);
  if (ret == 0) {
    CWX_ERROR(("ZkRoot node:%s not exists.", m_strZkRoot.c_str()));
    return -1;
  }
  if (-1 == ret) {
    CWX_ERROR(("Failure to getNodeChildren node:%s, err:%s", m_strZkRoot.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  if (childs.size()) {
    string strNode;
    string strValue;
    struct Stat stat;
    CWX_UINT32 uiDataLen;
    list<string> items;
    for(child_iter = childs.begin(); child_iter != childs.end(); child_iter++) {
      CwxZkMqGroup group;
      group.m_strGroup = *child_iter;
      do {
        ///处理该group的mq节点
        uiDataLen = MAX_ZK_DATA_SIZE;
        strNode = m_strZkRoot + "/" + group.m_strGroup + "/master";
        ret = m_zk->wgetNodeData(strNode, m_szZkDataBuf, uiDataLen, stat,
            CwxSubProxyZkHandler::watherMasterMq, this);
        if (-1 == ret) {
          CWX_ERROR(("Failure to get node:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
          return -1;
        }else if (0 == ret) {
          CWX_ERROR(("Node:%s not exists.", strNode.c_str()));
          break;
        }
        m_szZkDataBuf[uiDataLen] = 0x00;
        group.m_strMasterId = m_szZkDataBuf;
        ///获取mq实例节点配置信息
        uiDataLen = MAX_ZK_DATA_SIZE;
        strNode = m_strZkRoot + "/" + group.m_strGroup + "/mq/" + group.m_strMasterId;
        ret = m_zk->getNodeData(strNode, m_szZkDataBuf, uiDataLen, stat);
        if (-1 == ret) {
          CWX_ERROR(("Failure to get node:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
          return -1;
        }else if (0 == ret) {
          CWX_ERROR(("Node:%s not exist.", strNode.c_str()));
          break;
        }
        m_szZkDataBuf[uiDataLen] = 0x00;
        items.clear();
        if (3 != CwxCommon::split(string(m_szZkDataBuf), items,':')) {
          CWX_ERROR(("Node:%s value:%s is not in format[mq-conf:max-sid:timestamp]",
              strNode.c_str(), m_szZkDataBuf));
          break;
        }
        if (0 != _parseMqConf(*(items.begin()), group)) {
          CWX_ERROR(("Node:%s config invalid, %s", strNode.c_str(), m_szErr2K));
          break;
        }
      }while(0);
      CWX_INFO(("Group:%s, mast-id:%s, host:%s, port:%d", group.m_strGroup.c_str(),
          group.m_strMasterId.c_str(), group.m_masterHost.getHostName().c_str(),
          group.m_masterHost.getPort()));
      m_group[group.m_strGroup] = group;
    }
  }
  ///通知recv线程，节点发生变化
  noticeMasterMq();
  return 0;
}

///返回：0：成功；-1：失败
int CwxSubProxyZkHandler::_parseMqConf(string const& value, CwxZkMqGroup& host) {
  CwxIniParse parse;
  if (!parse.parse(value)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "value:%s invalid.", value.c_str());
    return -1;
  }
  string strValue;
  ///get outer_dispatch:host
  if (parse.getAttr("outer_dispatch", "host", strValue) && strValue.length()) {
     host.m_masterHost.setHostName(strValue);
  }else {
    CwxCommon::snprintf(m_szErr2K, 2047,
        "outer_dispatch:host empty, data:%s", strValue.c_str());
    return -1;
  }
  ///get outer_dispatch:port
  if (parse.getAttr("outer_dispatch", "port", strValue) && strValue.length()) {
    host.m_masterHost.setPort(strtoul(strValue.c_str(), NULL, 10));
  } else {
    CwxCommon::snprintf(m_szErr2K, 2047,
        "outer_dispatch port empty, data:%s",strValue.c_str());
    return -1;
  }
  return 0;
}

void CwxSubProxyZkHandler::watcherMqGroup(zhandle_t* ,
    int ,
    int ,
    const char* path,
    void* context) {
  CwxSubProxyZkHandler* zkHandler = (CwxSubProxyZkHandler*)context;
  char *node = new char[strlen(path) + 1];
  strcpy(node, path);
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(&node));
  memcpy(msg->wr_ptr(), &node, sizeof(&node));
  msg->wr_ptr(sizeof(&node));
  msg->event().setEvent(EVENT_ZK_GROUP_EVENT);
  zkHandler->m_pApp->getZkThreadPool()->append(msg);
}

void CwxSubProxyZkHandler::watherMasterMq(zhandle_t* ,
      int ,
      int ,
      const char* path,
      void* context) {
  CwxSubProxyZkHandler* zkHandler = (CwxSubProxyZkHandler*)context;
  char *node = new char[strlen(path) + 1];
  strcpy(node, path);
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(&node));
  memcpy(msg->wr_ptr(), &node, sizeof(&node));
  msg->wr_ptr(sizeof(&node));
  msg->event().setEvent(EVENT_ZK_MQ_EVENT);
  zkHandler->m_pApp->getZkThreadPool()->append(msg);
}

int CwxSubProxyZkHandler::_dealMqGroupEvent(CwxTss* ,
      CwxMsgBlock*& msg,
      CWX_UINT32 ) {
  char* node;
  memcpy(&node, msg->rd_ptr(), sizeof(&node));
  string strNode = node;
  delete node;
  list<string> childs;
  int ret = m_zk->wgetNodeChildren(strNode, childs, CwxSubProxyZkHandler::watcherMqGroup, this);
  if (-1 == ret) {
    CWX_ERROR(("Failure to get node:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
    m_group.clear();
    return -1;
  }else if((0 == ret) || (childs.size() == 0)){ ///根节点被删除了
    CWX_ERROR(("Node:%s no exists.", strNode.c_str()));
    m_group.clear();
    return 0;
  }
  for(map<string, CwxZkMqGroup>::iterator iter = m_group.begin(); iter != m_group.end();) {
    if (find(childs.begin(), childs.end(), iter->first) == childs.end()) {
      CWX_INFO(("Group:%s is deleted", iter->first.c_str()));
      m_group.erase(iter++);
      continue;
    }
    iter++;
  }
  CWX_UINT32 uiDataLen;
  struct Stat stat;
  list<string> items;
  for(list<string>::iterator iter = childs.begin(); iter != childs.end(); iter++) {
    if (m_group.find(*iter) != m_group.end()) continue;///新增节点
    CwxZkMqGroup group;
    group.m_strGroup = *iter;
    do{
      uiDataLen = MAX_ZK_DATA_SIZE;
      strNode = m_strZkRoot + "/" + group.m_strGroup + "/master";
      ret = m_zk->wgetNodeData(strNode, m_szZkDataBuf, uiDataLen, stat,
          CwxSubProxyZkHandler::watherMasterMq, this);
      if (-1 == ret) {
        CWX_ERROR(("Failure to get node:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
        return -1;
      }else if (0 == ret) {
        CWX_ERROR(("Node:%s not exists.", strNode.c_str()));
        break;
      }else {
        m_szZkDataBuf[uiDataLen] = 0x00;
        group.m_strMasterId = m_szZkDataBuf;
        ///获取节点的配置信息
        strNode =m_strZkRoot + "/" + group.m_strGroup + "/mq/" + group.m_strMasterId;
        uiDataLen = MAX_ZK_DATA_SIZE;
        ret = m_zk->getNodeData(strNode, m_szZkDataBuf, uiDataLen, stat);
        if (-1 == ret) {
          CWX_ERROR(("Failure to get node:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
          return -1;
        }else if (0 == ret) {
          CWX_ERROR(("Node:%s not exists.", strNode.c_str()));
          break;
        }
        m_szZkDataBuf[uiDataLen] = 0x00;
        items.clear();
        if (3 != CwxCommon::split(string(m_szZkDataBuf), items, ':')) {
          CWX_ERROR(("Node:%s value:%s is not in format[mq-conf:max-sid:timestamp]",
              strNode.c_str(), m_szZkDataBuf));
          break;
        }
        if (0 != _parseMqConf(*(items.begin()), group)) {
          CWX_ERROR(("Node:%s conf:%s invalid.", strNode.c_str(), m_szZkDataBuf));
          break;
        }
      }
    }while(0);
    m_group[group.m_strGroup] = group;
  }
  ///发送消息通知给recv线程
  noticeMasterMq();
  return 0;
}

int CwxSubProxyZkHandler::_dealMasterMqEvent(CwxTss* ,
      CwxMsgBlock*& msg,
      CWX_UINT32 ) {
  char* node;
  memcpy(&node, msg->rd_ptr(), sizeof(&node));
  list<string> items;
  list<string>::iterator iter;
  string strNode = node;
  delete node;
  if (4 != CwxCommon::split(strNode, items, '/')) {
    CWX_ERROR(("Node:%s is not in format /cwx-mq/group/master", strNode.c_str()));
    return 0;
  }
  CwxZkMqGroup group;
  iter = items.begin(); iter++; iter++;
  group.m_strGroup = *iter;
  CWX_UINT32 uiDataLen = MAX_ZK_DATA_SIZE;
  struct Stat stat;
  int ret = m_zk->wgetNodeData(strNode, m_szZkDataBuf, uiDataLen, stat,
      CwxSubProxyZkHandler::watherMasterMq, this);
  if (-1 == ret) {
    CWX_ERROR(("Failure to get node:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  do {
    if (0 == ret) {
      CWX_ERROR(("Node:%s not exists.", strNode.c_str()));
      break;
    }
    m_szZkDataBuf[uiDataLen] = 0x00;
    group.m_strMasterId = m_szZkDataBuf;
    strNode = m_strZkRoot + "/" + group.m_strGroup + "/mq/" + group.m_strMasterId;
    uiDataLen = MAX_ZK_DATA_SIZE;
    ret = m_zk->getNodeData(strNode, m_szZkDataBuf, uiDataLen, stat);
    if (-1 == ret) {
      CWX_ERROR(("Failure to get node:%s ,err:%s", strNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }else if (0 == ret) {
      CWX_ERROR(("Node:%s not exists.", strNode.c_str()));
      break;
    }
    items.clear();
    m_szZkDataBuf[uiDataLen] = 0x00;
    if (3 != CwxCommon::split(string(m_szZkDataBuf), items, ':')) {
      CWX_ERROR(("Node:%s, value:%s is not in format[mq-conf:max-sid:timestamp]", strNode.c_str(),
          m_szZkDataBuf));
      break;
    }
    if (0 != _parseMqConf(*(items.begin()), group)) {
      CWX_ERROR(("Node:%s value:%s config invalid, err:%s", strNode.c_str(),
          m_szZkDataBuf, m_szErr2K));
    }
  }while(0);
  m_group[group.m_strGroup] = group;
  CWX_INFO(("Mq-master change event: group:%s, master-id:%s, host:%s, port:%d",
      group.m_strGroup.c_str(), group.m_strMasterId.c_str(), group.m_masterHost.getHostName().c_str(),
      group.m_masterHost.getPort()));
  ////发送变更消息给主线程
  noticeMasterMq();
  return 0;
}

int CwxSubProxyZkHandler::checkMqGroup() {
  if (!m_group.size()) return 0;
  int ret;
  CWX_UINT32 uiDataLen;
  string strNode;
  list<string> items;
  bool bHasNew = false;
  struct Stat stat;
  map<string/*group*/, CwxZkMqGroup>::iterator iter = m_group.begin();
  for(; iter != m_group.end(); iter++) {
    if (!(iter->second.isMasterEmpty())) continue;
    CWX_INFO(("check group:%s is valid.", iter->first.c_str()));
    CwxZkMqGroup group;
    group.m_strGroup = iter->first;
    ///设置该group的master-mq信息
    strNode = m_strZkRoot + "/" + iter->first + "/master";
    uiDataLen = MAX_ZK_DATA_SIZE;
    ret = m_zk->wgetNodeData(strNode, m_szZkDataBuf, uiDataLen, stat,
        CwxSubProxyZkHandler::watherMasterMq, this);
    if (-1 == ret) {
      CWX_ERROR(("Failure to get node:%s ,err:%s", strNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }else if (0 == ret) {
      CWX_ERROR(("Node:%s not exist.", strNode.c_str()));
    }else { ///获取到节点信息
      m_szZkDataBuf[uiDataLen] = 0x00;
      group.m_strMasterId = m_szZkDataBuf;
      strNode = m_strZkRoot + "/" + iter->first + "/mq/" + group.m_strMasterId;
      uiDataLen = MAX_ZK_DATA_SIZE;
      ret = m_zk->getNodeData(strNode, m_szZkDataBuf, uiDataLen, stat);
      if (-1 == ret) {
        CWX_ERROR(("Failure to get node:%s , err:%s", strNode.c_str(), m_zk->getErrMsg()));
        return -1;
      }else if(0 == ret) {
        CWX_ERROR(("Node:%s not exists.", strNode.c_str()));
        continue;
      }
      m_szZkDataBuf[uiDataLen] = 0x00;
      items.clear();
      if (3 != CwxCommon::split(string(m_szZkDataBuf), items, ':')) {
        CWX_ERROR(("Node:%s value:%s is not in format[mq-conf:max-sid:timestamp]",
            strNode.c_str(), m_szZkDataBuf));
        continue;
      }
      if (0 != _parseMqConf(*(items.begin()), group)){
        CWX_ERROR(("Node:%s, value:%s invalid config", strNode.c_str(), m_szZkDataBuf));
        continue;
      }
      m_group[group.m_strGroup] = group;
      bHasNew = true;
      CWX_INFO(("Check valid group:%s, master-id:%s, master-host:%s, master-port:%d",
          group.m_strGroup.c_str(), group.m_strMasterId.c_str(),
          group.m_masterHost.getHostName().c_str(), group.m_masterHost.getPort()));
    }
  }
  if (bHasNew) {
    ///发送消息给recv线程
    noticeMasterMq();
  }
  return 0;
}

void CwxSubProxyZkHandler::noticeMasterMq() {
  map<string/*group*/, CwxZkMqGroup>::iterator iter = m_group.begin();
  map<string, CwxZkMqGroup>* group = new map<string, CwxZkMqGroup>;
  while( iter != m_group.end()) {
    if (!(iter->second.isMasterEmpty())) group->insert(pair<string, CwxZkMqGroup>(iter->first, iter->second));
    iter++;
  }
  m_pApp->setNewMqGroup(group);
}

