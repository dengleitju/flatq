#include "CwxMqZkHandler.h"
#include "CwxMqApp.h"

CwxMqZkHandler::CwxMqZkHandler(CwxMqApp* app) : m_pApp(app) {
  m_zk = NULL;
  m_zkLocker = NULL;
  m_bInit = false;
  m_bConnected = false;
  m_bAuth = false;
  m_bValid = false;
  m_clientId = NULL;
  m_strZkGroupNode = app->getConfig().getZk().m_strRootPath
      + app->getConfig().getZk().m_strGroup;
  m_strZkMqNode = m_strZkGroupNode + "/mq";
  m_strZkMasterMq = m_strZkGroupNode + "/master";
  m_strZkTopicNode = m_strZkGroupNode + "/topic";
  m_strZkDelTopicNode = m_strZkGroupNode + "/deleted";
  strcpy(m_szErr2K, "Not init.");
  m_ullVersion = 1;
}

CwxMqZkHandler::~CwxMqZkHandler() {
  stop();
  if (m_clientId) {
    delete m_clientId;
    m_clientId = NULL;
  }
}

void CwxMqZkHandler::stop() {
  CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
  if (m_zk) {
    m_zk->disconnect();
  }
  if (m_zkLocker) {
    m_zkLocker->unlock();
    delete m_zkLocker;
    m_zkLocker = NULL;
  }
  if (m_zk) {
    delete m_zk;
    m_zk = NULL;
  }
}

int CwxMqZkHandler::init() {
  CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
  return _init();
}

int CwxMqZkHandler::_init() {
  _reset();
  ///形成mq节点data数据[recv/dispatch配置信息]:[max_sid|timestamp]
  char szTmp[256];
  CwxCommon::snprintf(szTmp,
      255,
      "[recv]\r\n"\
      "host=%s\r\n"\
      "port=%d\r\n"\
      "[inner_dispatch]\r\n"\
      "host=%s\r\n"\
      "port=%d\r\n"\
      "[outer_dispatch]\r\n"\
      "host=%s\r\n"\
      "port=%d\r\n",
      m_pApp->getConfig().getRecv().m_recv.getHostName().c_str(),
      m_pApp->getConfig().getRecv().m_recv.getPort(),
      m_pApp->getConfig().getInnerDispatch().m_async.getHostName().c_str(),
      m_pApp->getConfig().getInnerDispatch().m_async.getPort(),
      m_pApp->getConfig().getOuterDispatch().m_async.getHostName().c_str(),
      m_pApp->getConfig().getOuterDispatch().m_async.getPort()
      );
  m_strMqZkConfig = szTmp;
  CwxCommon::snprintf(szTmp, 255,"%s:0:0",
      m_strMqZkConfig.c_str());
  m_zk = new ZkAdaptor(m_pApp->getConfig().getZk().m_strZkServer);
  if (0 != m_zk->init()) {
    CwxCommon::snprintf(m_szErr2K, 2048, "Failure to invoke ZkAdaptor::init(), err:%s", m_zk->getErrMsg());
    CWX_ERROR((m_szErr2K));
    return -1;
  }
  if (0 != _connect()) return -1;
  ///初始化锁
  m_zkLocker = new ZkLocker();
  if (0 != m_zkLocker->init(m_zk,
      m_strZkMqNode,
      m_pApp->getConfig().getCommon().m_strMqId,
      lock_complete,
      this,
      szTmp,
      strlen(szTmp))) {
    CwxCommon::snprintf(m_szErr2K, 2048, "Failure to init zk lock");
    CWX_ERROR((m_szErr2K));
    return -1;
  }
  m_bInit = true;
  return 0;
}

void CwxMqZkHandler::_reset() {
  m_bInit = false;
  m_bConnected = false;
  m_bAuth = false;
  m_bValid = false;
  strcpy(m_szErr2K, "No init.");
  if (m_zkLocker) {
    m_zkLocker->unlock();
  }
  if (m_zk) {
    m_zk->disconnect();
  }
  if (m_zkLocker) {
    delete m_zkLocker;
    m_zkLocker = NULL;
  }
  if (m_zk) {
    delete m_zk;
    m_zk = NULL;
  }
}

int CwxMqZkHandler::_dealTimeoutEvent(CwxTss* , CwxMsgBlock*&, CWX_UINT32 ) {
  if (!m_bInit) {
    if (m_lock.m_strMaster.length()) {
      m_lock.m_strMaster = "";
      m_lock.m_strMasterInnerDispHost = "";
      m_lock.m_unMasterInnerDispPort = 0;
      m_lock.m_strPrev = "";
      m_lock.m_strPrevInnerDispHost = "";
      m_lock.m_unPrevInnerDispPort = 0;
      m_lock.m_ullPrevMasterMaxSid = 0;
      m_lock.m_bMaster = false;
      _noticeLockChange();
    }
    _init();
  }
  if (m_bValid) {
    _setMqInfo(); ///设置mq实例节点
    if (m_zkLocker && m_zkLocker->isLocked()) {
      _setMasterInfo(); ///设置Master信息
      _processTopicInfo(); ///处理topic信息
      _setSourceInfo(); ///设置source信息
    }
  }
  return 0;
}

int CwxMqZkHandler::_dealConnectedEvent(CwxTss*, CwxMsgBlock*& msg, CWX_UINT32 ) {
  int ret = 0;
  if (!m_bInit) return 0; ///如果未init,则忽略
  ///连接成功
  m_bConnected = true;
  ///保存zk的client id
  if (msg->length()) {
    if (!m_clientId) m_clientId = new clientid_t;
    memcpy(m_clientId, msg->rd_ptr(), sizeof(clientid_t));
  }else{
    if (m_clientId) delete m_clientId;
    m_clientId = NULL;
  }
  ///鉴权，若失败则返回
  ret = _auth();
  if (-1 == ret) {///重新初始化
    return -1;
  }
  if (1 == ret) {
    if (0 != _lock()) {
      return -1;
    }
    m_bValid = true;
  }
  return 0;
}

void CwxMqZkHandler::doEvent(CwxTss* tss, CwxMsgBlock*& msg, CWX_UINT32 uiLeftEvent) {
  CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
  bool bSuccess = false;
  do{
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
      if (0 != _lock()) break;
      if (m_bInit) m_bValid = true;
    }else if (EVENT_ZK_LOCK_CHANGE == msg->event().getEvent()) {
      if (0 != _loadLockInfo(tss)) break;
    }else if (EVENT_ZK_LOST_WATCH == msg->event().getEvent()) {
      if (0 != _lock()) break;
    }else {
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
int CwxMqZkHandler::_connect() {
  if (0 != m_zk->connect(m_clientId, 0, CwxMqZkHandler::watcher, this)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "Failure to invoke ZkAdaptor::connect(), err:%s", m_zk->getErrMsg());
    CWX_ERROR((m_szErr2K));
    return -1;
  }
  return 0;
}

void CwxMqZkHandler::watcher(zhandle_t* ,
      int type,
      int state,
      const char* path,
      void* context) {
  CwxMqZkHandler* zkHandler = (CwxMqZkHandler*)context;
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
  }else if (type == ZOO_CREATED_EVENT) {
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

///设置mq信息
int CwxMqZkHandler::_setMqInfo() {
  if (!m_bInit) return 0; ///如果未init,则忽略
  if (!m_zk) return 0;
  string strPathNode;
  m_zkLocker->getSelfPathNode(strPathNode); ///获取mq对应的实例节点
  char szTmp[256];
  char szSid[64];
  CwxCommon::snprintf(szTmp, 255,"%s:%s:%d",
      m_strMqZkConfig.c_str(),
      CwxCommon::toString(m_pApp->getCurSid(), szSid, 10),
      m_pApp->getCurTimestamp());
  int ret = m_zk->setNodeData(strPathNode, szTmp, strlen(szTmp));
  if (1 == ret) return 0;
  if (-1 == ret) {
    CWX_ERROR(("Failure to set node:%s, err=%s", strPathNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  //now, it's zero.节点必须存在的
  CWX_ERROR(("Failure to set node for not existing, node:%s", strPathNode.c_str()));
  return -1;
}


int CwxMqZkHandler::_setSourceInfo() {
  ///设置管辖的source信息
  map<string, map<string, CwxMqZkSource> > source;
  m_pApp->dumpSource(source);
  map<string, map<string, CwxMqZkSource> >::iterator iter = source.begin();
  string strTopic;
  char szSid[64];
  uint32_t uiNow = time(NULL);
  while(iter != source.end()) {
    strTopic = iter->first;
    ///对于已经删除的topic/source不做设置
    if (m_allTopicState.find(strTopic) == m_allTopicState.end() ||
        m_allTopicState[strTopic] == CWX_MQ_TOPIC_DELETE) {
      iter++;
      continue;
    }
    for(map<string, CwxMqZkSource>::iterator iter2 = iter->second.begin();
        iter2 != iter->second.end(); iter2++) {
      string strNode = m_strZkTopicNode + "/" + strTopic + "/" + iter2->first;
      CwxCommon::snprintf(m_szZkDataBuf, MAX_ZK_DATA_SIZE, "%s:%u:%u",
          CwxCommon::toString(iter2->second.m_ullSid, szSid, 10),
          iter2->second.m_ullLastNum,
          uiNow);
      int iRet = m_zk->setNodeData(strNode, m_szZkDataBuf, strlen(m_szZkDataBuf));
      if (0 == iRet) {
        ///CWX_ERROR(("Set source node data not exist:%s", strNode.c_str()));
        iRet = m_zk->createNode(strNode, m_szZkDataBuf, strlen(m_szZkDataBuf));
        if (-1 == iRet) {
          CWX_ERROR(("Failure create node:%s ,err:%s", strNode.c_str(), m_zk->getErrMsg()));
        }
      }else if (-1 == iRet) {
        CWX_ERROR(("Set source node data:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
      }
    }
    iter++;
  }
  return 0;
}

int CwxMqZkHandler::_processTopicInfo() {
  if (!m_bInit) return 0; ///未init，则忽略
  if (!m_zk) return 0;
  list<string> childs;
  list<CwxMqZkTopicSid> topicSids;
  map<string, CWX_UINT8> topicState;
  int ret;
  m_pApp->getTopicMgr()->dumpTopicInfo(topicSids);
  {///设置topic信息
    CWX_UINT32 uiLen = 0;
    char szSid[64];
    for(list<CwxMqZkTopicSid>::iterator iter = topicSids.begin(); iter != topicSids.end(); iter++) {
      snprintf(m_szZkDataBuf + uiLen, MAX_ZK_DATA_SIZE - uiLen -1, "%s|%s|%u|%d:",
          iter->m_strTopic.c_str(), CwxCommon::toString(iter->m_ullSid, szSid, 10), iter->m_uiTimestamp,iter->m_state);
      uiLen = strlen(m_szZkDataBuf);
      topicState[iter->m_strTopic] = iter->m_state;
    }
    ret = m_zk->setNodeData(m_strZkTopicNode, m_szZkDataBuf, strlen(m_szZkDataBuf));
    if (0 == ret) {
      CWX_ERROR(("Topic node:%s not exist.", m_strZkTopicNode.c_str()));
      return 0;
    }
    if (-1 == ret) {
      CWX_ERROR(("Failure to set topic node:%s data, err:%s", m_strZkTopicNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }
  }
  {///获取topic信息
    string strNode = m_strZkTopicNode;
    ret = m_zk->getNodeChildren(strNode, childs);
    if (-1 == ret) {
      CWX_ERROR(("Failure to get node:%s child, err=%s", strNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }else if (0 == ret) {
      CWX_ERROR(("Topic node:%s not exist.", strNode.c_str()));
      return 0;
    }
    struct Stat stats;
    CWX_UINT32 uiLen = MAX_ZK_DATA_SIZE;
    m_allTopicState.clear();
    for(list<string>::iterator iter = childs.begin(); iter != childs.end();) {
      strNode = m_strZkTopicNode + "/" + *iter;
      if (1 !=  (ret = m_zk->getNodeData(strNode,
          m_szZkDataBuf,
          uiLen,
          stats))) {
        if (0 != ret) {
          CWX_ERROR(("Failure to get topic node :%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
        }else {
          CWX_ERROR(("Topic node[%s] doesn't exist.", strNode.c_str()));
        }
        continue;
      }
      m_szZkDataBuf[uiLen] = 0x00;
      CWX_UINT8 state = CWX_MQ_TOPIC_NORMAL;
      if (uiLen) {
        state = strtoul(m_szZkDataBuf, NULL, 10);
      }
      m_allTopicState.insert(pair<string, CWX_UINT8>(*iter, state));
      iter++;
    }
    map<string, CWX_UINT8>* changedTopics = new map<string, CWX_UINT8>;
    if (0 != m_pApp->getTopicMgr()->updateTopicBinlogMgr(m_allTopicState,
        *changedTopics,
        m_pApp->getConfig().getBinLog().m_uiBinLogMSize,
        m_pApp->getConfig().getBinLog().m_uiFlushNum,
        m_pApp->getConfig().getBinLog().m_uiFlushSecond,
        m_pApp->getConfig().getBinLog().m_uiSaveFileDay,
        true,
        m_szErr2K)) {
      CWX_ERROR(("Failure to updateTopicBinlogMgr, err:%s", m_szErr2K));
      return -1;
    }
    ///发送新增topic到内部分发线程
    if (changedTopics->size()) {
      CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(changedTopics));
      memcpy(msg->wr_ptr(), &changedTopics, sizeof(changedTopics));
      msg->wr_ptr(sizeof(changedTopics));
      msg->event().setSvrId(CwxMqApp::SVR_TYPE_INNER_DISP);
      msg->event().setEvent(EVENT_ZK_TOPIC_CHANGE);
      m_pApp->getInnerDispatchThreadPool()->append(msg);
    }else {
      delete changedTopics;
    }
  }
  return 0;
}

int CwxMqZkHandler::_setMasterInfo() {
  if (!m_bInit) return 0; ///未init，则忽略
  if (!m_zk) return 0;
  string strTime;
  string lockNode;
  m_zkLocker->getSelfNode(lockNode);
  char szTmp[512];
  char szSid1[64];
  char szSid2[64];
  CwxCommon::snprintf(szTmp,
      511,
      "%s:%s:%s:%u",
      lockNode.c_str(),///m_pApp->getConfig().getCommon().m_strMqId.c_str(),
      CwxCommon::toString(m_pApp->getStartSid(), szSid1, 10),
      CwxCommon::toString(m_pApp->getCurSid(), szSid2, 10),
      m_pApp->getCurTimestamp());
  int ret = m_zk->setNodeData(m_strZkMqNode, szTmp, strlen(szTmp));
  if (0 == ret) return 0;
  if (-1 == ret) {
    CWX_ERROR(("Failure to set mq-master node:%s data, err:%s", m_strZkTopicNode.c_str(), m_zk->getErrMsg()));
  }else if (0 == ret){
    CWX_ERROR(("Failure to set mq-master node:%s, is not exist.", m_strZkTopicNode.c_str()));
  }
  return -1;
}

///锁回调函数
void CwxMqZkHandler::lock_complete(bool, void* context) {
  CwxMqZkHandler* zk = (CwxMqZkHandler*)context;
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
  msg->event().setEvent(EVENT_ZK_LOCK_CHANGE);
  msg->event().setSvrId(CwxMqApp::SVR_TYPE_ZK);
  zk->m_pApp->getZkThreadPool()->append(msg);
}

///1:认证成功；0：等待认证结果；-1：认证失败
int CwxMqZkHandler::_auth() {
  if (!m_bInit) return 0; ///没有init,则忽略
  if (m_pApp->getConfig().getZk().m_strAuth.length()) {
    if (!m_zk->addAuth("digest",
        m_pApp->getConfig().getZk().m_strAuth.c_str(),
        m_pApp->getConfig().getZk().m_strAuth.length(),
        0,
        CwxMqZkHandler::zkAuthCallback,
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

int CwxMqZkHandler::_lock() {
  if (!m_bInit) return 0; ///如果未init，则忽略
  if (!m_zk) return 0;
  if (0 != _createRootNode()) return -1;
  this->_loadSourceInfo(); ///加载source信息
  this->_processTopicInfo(); ///获取topic信息
  CWX_INFO(("Lock........."));
  if (0 != m_zkLocker->lock(ZkLocker::ZK_WATCH_TYPE_ROOT)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "Failure to lock zookeeper node.");
    CWX_ERROR((m_szErr2K));
    return -1;
  }
  return 0;
}

int CwxMqZkHandler::_createRootNode() {
  struct Stat stat;
  ///struct ACL_vector acl;
  struct ACL_vector *pcal=&ZOO_OPEN_ACL_UNSAFE;
  ///创建group节点
  int iRet = m_zk->nodeExists(m_strZkGroupNode, stat);
  if (-1 == iRet) {
    CWX_ERROR(("Failure to nodeExist node:%s, err:%s", m_strZkGroupNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  if (0 == iRet) {
    iRet = m_zk->createNode(m_strZkGroupNode, NULL, 0, pcal, 0, true);
    if (-1 == iRet) {
      CWX_ERROR(("Failure to create node:%s, err:%s", m_strZkGroupNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }
  }
  ///创建master-mq节点
  iRet = m_zk->nodeExists(m_strZkMasterMq, stat);
  if (-1 == iRet) {
    CWX_ERROR(("Failure to nodeExist node:%s, err:%s", m_strZkMasterMq.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  if (0 == iRet) {
    iRet = m_zk->createNode(m_strZkMasterMq, NULL, 0, pcal, 0, true);
    if (-1 == iRet) {
      CWX_ERROR(("Failure to create node:%s, err:%s", m_strZkMasterMq.c_str(), m_zk->getErrMsg()));
      return -1;
    }
  }
  ///创建mq根节点
  iRet = m_zk->nodeExists(m_strZkMqNode, stat);
  if (-1 == iRet) {
    CWX_ERROR(("Failure to nodeExist node:%s, err:%s", m_strZkMqNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  if (0 == iRet) {
    iRet = m_zk->createNode(m_strZkMqNode, NULL, 0, pcal, 0, true);
    if (-1 == iRet) {
      CWX_ERROR(("Failure to create node:%s, err:%s", m_strZkMqNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }
  }
  ///创建topic根节点
  iRet = m_zk->nodeExists(m_strZkTopicNode, stat);
  if (-1 == iRet) {
    CWX_ERROR(("Failure to nodeExist node:%s, err:%s", m_strZkTopicNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  if (0 == iRet) {
    iRet = m_zk->createNode(m_strZkTopicNode, NULL, 0, pcal, 0, true);
    if (-1 == iRet) {
      CWX_ERROR(("Failure to create node:%s, err:%s", m_strZkTopicNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }
  }
  ///创建deleted topic根节点
  iRet = m_zk->nodeExists(m_strZkDelTopicNode, stat);
  if (-1 == iRet) {
    CWX_ERROR(("Failure to nodeExist node:%s, err:%s", m_strZkDelTopicNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  if (0 == iRet) {
    iRet = m_zk->createNode(m_strZkDelTopicNode, NULL, 0, pcal, 0, true);
    if (-1 == iRet) {
      CWX_ERROR(("Failure to create node:%s, err:%s", m_strZkDelTopicNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }
  }
  return 0;
}

void CwxMqZkHandler::zkAuthCallback(int rc,
      const void* data) {
  CwxMqZkHandler* zk = (CwxMqZkHandler*)data;
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
  msg->event().setSvrId(CwxMqApp::SVR_TYPE_ZK);
  msg->event().setEvent(ZOK==rc?EVENT_ZK_SUCCESS_AUTH:EVENT_ZK_FAIL_AUTH);
  zk->m_pApp->getZkThreadPool()->append(msg);
}

///获取锁信息；0：成功；-1：失败
int CwxMqZkHandler::_loadLockInfo(CwxTss*) {
  string strValue;
  string strSession;
  uint64_t seq;
  CwxMqZkLock lock;
  if (!m_bInit) return 0; ///未init，则忽略
  if (!m_zk) return 0;
  lock.m_bMaster = m_zkLocker->isLocked();
  m_zkLocker->getOwnerNode(strValue);

  CWX_INFO(("Lock change, master=%s, master_id=%s",
      lock.m_bMaster?"yes":"no",
      strValue.length()?strValue.c_str():""));
  if (strValue.length()) {
    //获取该node的mq_id
    if (!ZkLocker::splitSeqNode(strValue, seq, lock.m_strMaster, strSession)) {
      return -1;
    }
    //获取该mq实例节点dispatch配置信息
    if (0 != _getMqDispatchHost(strValue, lock.m_strMasterInnerDispHost, lock.m_unMasterInnerDispPort))
      return -1;
  }else {
    return -1;
  }
  m_zkLocker->getPrevNode(strValue);
  if (strValue.length()) {
    if (!ZkLocker::splitSeqNode(strValue, seq, lock.m_strPrev, strSession)) {
      return -1;
    }
    ///获取该mq实例节点dispatch配置信息
    if (0 != _getMqDispatchHost(strValue, lock.m_strPrevInnerDispHost, lock.m_unPrevInnerDispPort))
      return -1;
  }else {
    lock.m_strPrev = "";
    lock.m_strPrevInnerDispHost = "";
    lock.m_unPrevInnerDispPort = 0;
  }
  if (lock.m_bMaster && !m_lock.m_bMaster) { ///变为master
    string strMaster;
    CWX_UINT64 ullStartSid = 0;
    CWX_UINT64 ullMaxSid = 0;
    string strTimestamp;
    char szTmp1[64];
    char szTmp2[64];
    char szTmp3[64];
    if (0 != _getMasterInfo(strMaster, ullStartSid, ullMaxSid, strTimestamp)) {
      return -1;
    }
    //设置前一个的最大sid
    lock.m_ullPrevMasterMaxSid = ullMaxSid;
    ///设置前一个的最大sid
    ///lock.m_ullPrevMasterMaxSid = ullSid;
    CWX_INFO(("I become master, last master info:master[%s], start_sid(%s), max_sid(%s), mine_max_sid(%s)",
        strMaster.length()?strMaster.c_str():"",
        CwxCommon::toString(ullStartSid, szTmp1, 10),
        CwxCommon::toString(ullMaxSid, szTmp2, 10),
        CwxCommon::toString(m_pApp->getCurSid(), szTmp3, 10)));
    if (strMaster != m_pApp->getConfig().getCommon().m_strMqId) { ///不是我自己
      if (m_pApp->getCurSid() < ullStartSid || (ullMaxSid > m_pApp->getCurSid() &&
          ullMaxSid - m_pApp->getCurSid() > CWX_MQ_MASTER_SWITCH_SID_INC)) {
        CWX_INFO(("I can't become master, master_start_sid(%s), master_max_sid(%s), mine_max_sid(%s)",
           szTmp1, szTmp2, szTmp3));
        return -1;
      }
    }
    ///变为master,设置master的信息
    ///设置master节点
    string lockNode;
    m_zkLocker->getSelfNode(lockNode);
    int ret = m_zk->setNodeData(m_strZkMasterMq, lockNode.c_str(), lockNode.length());
    if (-1 == ret) {
      CWX_ERROR(("Failure to set master node:%s data, err:%s", m_strZkMasterMq.c_str(), m_zk->getErrMsg()));
      return -1;
    }else if (0 == ret) {
      CWX_ERROR(("Mq master node:%s is not exists.", m_strZkMasterMq.c_str(), m_zk->getErrMsg()));
      return -1;
    }
  } else {
    lock.m_ullPrevMasterMaxSid = m_lock.m_ullPrevMasterMaxSid;
  }
  ///CWX_INFO(("now[master:%s, prv:%s], befor[master:%s, prev:%s]",
      ///lock.m_strMaster.c_str(), lock.m_strPrev.c_str(), m_lock.m_strMaster.c_str(), lock.m_strPrev.c_str()));
  if (!(m_lock == lock)) {
    m_lock = lock;
    _noticeLockChange();
  }
  return 0;
}

int CwxMqZkHandler::_getMqDispatchHost(string strNode,
    string& inner_ip,
    CWX_UINT16& inner_port) {
  string strNodePath = m_strZkMqNode + "/" + strNode;
  struct Stat stat;
  CWX_UINT32 uiLen = MAX_ZK_DATA_SIZE;
  int iRet = m_zk->getNodeData(strNodePath, m_szZkDataBuf, uiLen, stat);
  if (1 != iRet) {
    CWX_ERROR(("Failure to get mq node :%s info, err:%s", strNodePath.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  m_szZkDataBuf[uiLen] = 0x00;
  list<string> items;
  list<string>::iterator iter;
  CwxCommon::split(string(m_szZkDataBuf), items, ':');
  if (3 != items.size()) {
    CWX_ERROR(("Mq[%s]'s data is not in format[conf:sid:timestamp] :%s", strNode.c_str(), m_szZkDataBuf));
    return -1;
  }
  iter = items.begin();
  CwxIniParse parse;
  if (!parse.parse(iter->c_str())) {
    CwxCommon::snprintf(m_szErr2K, 2048, "Failure to parse mq[%s] conf, data=%s, err=%s", strNode.c_str(), m_szZkDataBuf ,parse.getErrMsg());
    CWX_ERROR((m_szErr2K));
    return -1;
  }
  string strValue;
  ///get inner_dispatch:ip
  if (parse.getAttr("inner_dispatch", "host", strValue) && strValue.length()) {
    inner_ip = strValue;
  }else {
    CWX_ERROR(("Mq[%s] inner_dispatch:host empty, data:%s", strNode.c_str(), iter->c_str()));
    return -1;
  }
  ///get inner_dispatch:port
  if (parse.getAttr("inner_dispatch", "port", strValue) && strValue.length()) {
    inner_port = strtoul(strValue.c_str(), NULL, 10);
  } else {
    CWX_ERROR(("Mq[%s] inner_dispatch port empty, data:%s", strNode.c_str(), iter->c_str()));
    return -1;
  }
  return 0;
}

int CwxMqZkHandler::_getMasterInfo(string& strMaster,
    CWX_UINT64& ullStartSid,
    CWX_UINT64& ullMaxSid,
    string& strTimestamp) {
  strMaster.erase();
  ullStartSid = 0;
  ullMaxSid = 0;
  strTimestamp.erase();

  if (!m_bInit) return 0; ///如果未init，则忽略
  if (!m_zk) return 0;
  string strNode = m_strZkMqNode;
  char szTmp[512];
  struct Stat stat;
  CWX_UINT32 uiLen = 511;
  int ret = m_zk->getNodeData(strNode, szTmp, uiLen, stat);
  szTmp[uiLen] = 0x00;
  if (1 == ret) {
    list<string> items;
    list<string>::iterator iter;
    CwxCommon::split(string(szTmp), items, ':');
    if (4 != items.size()) {
      CWX_ERROR(("master's data isn't in format:[host:start_sid:sid:timestamp], node:%s, data:%s",
          strNode.c_str(), szTmp));
      return 0;
    }
    iter = items.begin();
    strMaster = *iter; iter++;
    ullStartSid = strtoull(iter->c_str(), NULL, 10);
    iter++;
    ullMaxSid = strtoull(iter->c_str(), NULL, 10);
    iter++;
    strTimestamp = *iter;
    return 0;
  }
  if (0 == ret) {
    CWX_ERROR(("Master node doesn't exist, node:%s", strNode.c_str()));
    return 0;
  }
  CWX_ERROR(("Failure to fetch master node's data, node:%s, err=%s", strNode.c_str(), m_zk->getErrMsg()));
  return 0;
}
//通知mq节点发生变化
void CwxMqZkHandler::_noticeLockChange() {
  CwxMsgBlock* msg = NULL;
  CwxMqZkLock* lock = NULL;
  m_ullVersion++;
  m_lock.m_ullVersion = m_ullVersion;
  ///通知写线程
  lock = new CwxMqZkLock(m_lock);
  CWX_INFO(("Notice lock change, master[%s:%s:%d], prev[%s:%s:%d]",
      lock->m_strMaster.c_str(), lock->m_strMasterInnerDispHost.c_str(),
      lock->m_unMasterInnerDispPort, lock->m_strPrev.c_str(),
      lock->m_strPrevInnerDispHost.c_str(), lock->m_unPrevInnerDispPort));
  msg = CwxMsgBlockAlloc::malloc(sizeof(&lock));
  memcpy(msg->wr_ptr(), &lock, sizeof(&lock));
  msg->wr_ptr(sizeof(&lock));
  msg->event().setSvrId(CwxMqApp::SVR_TYPE_RECV);
  msg->event().setEvent(EVENT_ZK_LOCK_CHANGE);
  m_pApp->getRecvThreadPool()->append(msg);
}

int CwxMqZkHandler::_loadSourceInfo() {
  ///获取topic下的source信息
  list<string> topics;
  list<string> items;
  struct Stat stat;
  list<string>::iterator iter;
  CwxMqZkSource zkSource;
  int iRet = m_zk->getNodeChildren(m_strZkTopicNode, topics);
  if (-1 == iRet) {
    CWX_ERROR(("Failure to get topic node child:%s, err:%s", m_strZkTopicNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  for(list<string>::iterator iter = topics.begin(); iter != topics.end(); iter++) {
    string strNode = m_strZkTopicNode + "/" + *iter;
    list<string> sources;
    iRet = m_zk->getNodeChildren(strNode, sources);
    if (-1 == iRet) {
      CWX_ERROR(("Failure to get topic source node child:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }
    if(1 == iRet) {
      for(list<string>::iterator source_iter = sources.begin(); source_iter != sources.end(); source_iter++) {
        string strSourceNode = strNode + "/" + *source_iter;
        CWX_UINT32 len = MAX_ZK_DATA_SIZE;
        iRet = m_zk->getNodeData(strSourceNode, m_szZkDataBuf,len, stat);
        if (-1 == iRet) {
          CWX_ERROR(("Failure to get source node data:%s, err:%s", strSourceNode.c_str(), m_zk->getErrMsg()));
          return -1;
        }
        if (1 == iRet && len > 0) {
          string strValue = m_szZkDataBuf;
          CwxCommon::split(strValue, items, ':');
          if (items.size() != 3) {
            CWX_ERROR(("Source node[%s] data:%s not in format [sid:last_num:update_time]", strSourceNode.c_str(), m_szZkDataBuf));
            continue;
          }
          list<string>::iterator item_iter = items.begin();
          zkSource.m_strSource = *source_iter;
          zkSource.m_ullSid = strtoull(item_iter->c_str(), NULL, 10);
          item_iter++;
          zkSource.m_ullLastNum = strtoull(item_iter->c_str(), NULL, 10);
          m_pApp->updateSource(*iter, zkSource);
        }
      }
    }
  }
  return 0;
}
