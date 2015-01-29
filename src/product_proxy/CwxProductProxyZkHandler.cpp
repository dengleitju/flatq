#include "CwxProductProxyZkHandler.h"
#include "CwxProductProxyApp.h"

CwxProProxyZkHandler::CwxProProxyZkHandler(CwxProProxyApp* app,
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

CwxProProxyZkHandler::~CwxProProxyZkHandler() {
  stop();
  if (m_clientId) {
    delete m_clientId;
    m_clientId = NULL;
  }
}

void CwxProProxyZkHandler::stop() {
  CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
  if (m_zk) {
    m_zk->disconnect();
    delete m_zk;
    m_zk = NULL;
  }
}

int CwxProProxyZkHandler::init() {
  CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
  return _init();
}

int CwxProProxyZkHandler::_init() {
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

void CwxProProxyZkHandler::_reset() {
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
  m_topic.clear();
}

int CwxProProxyZkHandler::_dealTimeoutEvent(CwxTss*, CwxMsgBlock*&, CWX_UINT32) {
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

int CwxProProxyZkHandler::_dealConnectedEvent(CwxTss*, CwxMsgBlock*& msg, CWX_UINT32) {
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


void CwxProProxyZkHandler::doEvent(CwxTss* tss, CwxMsgBlock* msg, CWX_UINT32 uiLeftEvent) {
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
    }else if (EVENT_ZK_TOPIC_CHANGE == msg->event().getEvent()){
      ///topic-group节点变化
      if (0 != _dealTopicEvent(tss, msg, uiLeftEvent)) break;
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
int CwxProProxyZkHandler::_connect() {
  if (0 != m_zk->connect(m_clientId, 0, CwxProProxyZkHandler::watcher, this)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "Failure to invoke ZkAdaptor::connect(), err:%s", m_zk->getErrMsg());
    CWX_ERROR((m_szErr2K));
    return -1;
  }
  return 0;
}


///1:认证成功；0：等待认证结果；-1：认证失败
int CwxProProxyZkHandler::_auth() {
  if (!m_bInit) return 0; ///没有init,则忽略
  if (m_pApp->getConfig().getZk().m_strAuth.length()) {
    if (!m_zk->addAuth("digest",
        m_pApp->getConfig().getZk().m_strAuth.c_str(),
        m_pApp->getConfig().getZk().m_strAuth.length(),
        0,
        CwxProProxyZkHandler::zkAuthCallback,
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

void CwxProProxyZkHandler::watcher(zhandle_t* ,
      int type,
      int state,
      const char* path,
      void* context) {
  CwxProProxyZkHandler* zkHandler = (CwxProProxyZkHandler*)context;
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

void CwxProProxyZkHandler::zkAuthCallback(int rc,
      const void* data) {
  CwxProProxyZkHandler* zk = (CwxProProxyZkHandler*)data;
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
  msg->event().setSvrId(CwxProProxyApp::SVR_PRO_TYPE_ZK);
  msg->event().setEvent(ZOK==rc?EVENT_ZK_SUCCESS_AUTH:EVENT_ZK_FAIL_AUTH);
  zk->m_pApp->getZkThreadPool()->append(msg);
}


int CwxProProxyZkHandler::_watch() {
  if (!m_bInit) return 0; ///如果未init,则忽略
  if (!m_zk) return 0;
  m_group.clear();
  m_topic.clear();
  list<string> childs;
  list<string>::iterator child_iter;
  ///设置对根目录监控
  int ret = m_zk->wgetNodeChildren(m_strZkRoot.c_str(), childs,
      CwxProProxyZkHandler::watcherMqGroup, this);
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
      CwxMqGroup group;
      group.m_strGroup = *child_iter;
      do {
        ///处理该group的mq节点
        uiDataLen = MAX_ZK_DATA_SIZE;
        strNode = m_strZkRoot + "/" + group.m_strGroup + "/master";
        ret = m_zk->wgetNodeData(strNode, m_szZkDataBuf, uiDataLen, stat,
            CwxProProxyZkHandler::watherMasterMq, this);
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
        ///获取该group的toic节点信息
        strNode = m_strZkRoot + "/" + group.m_strGroup + "/topic";
        list<string> topic;
        ret = m_zk->wgetNodeChildren(strNode, topic,
            CwxProProxyZkHandler::watcherMqTopic, this);
        if (ret == 0) {
          CWX_ERROR(("Node:%s not exist.", strNode.c_str()));
          break;
        }else if (-1 == ret) {
          CWX_ERROR(("Failure to get node:%s child list, err:%s", strNode.c_str(), m_zk->getErrMsg()));
          return -1;
        }
        if (topic.size()) {
          for(list<string>::iterator titer = topic.begin(); titer != topic.end(); titer++) {
            map<string/*topic*/, CwxMqTopicGroup >::iterator tp = m_topic.find(*titer);
           if (tp == m_topic.end()) {
             m_topic.insert(pair<string, CwxMqTopicGroup>(*titer, CwxMqTopicGroup(*titer)));
             tp = m_topic.find(*titer);
           }
           tp->second.addGroup(group.m_strGroup);
          }
        }
        group.setWathTopic();
      }while(0);
      CWX_INFO(("Group:%s, mast-id:%s, host:%s, port:%d", group.m_strGroup.c_str(),
          group.m_strMasterId.c_str(), group.m_strMasterHost.c_str(), group.m_unMasterPort));
      m_group[group.m_strGroup] = group;
    }
  }
  ///通知recv线程，节点发生变化
  noticeMasterMq();
  ///通知topic变化信息
  noticeTopicChange();
  return 0;
}

///返回：0：成功；-1：失败
int CwxProProxyZkHandler::_parseMqConf(string const& value, CwxMqGroup& host) {
  CwxIniParse parse;
  if (!parse.parse(value)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "value:%s invalid.", value.c_str());
    return -1;
  }
  string strValue;
  ///get recv:ip
  if (parse.getAttr("recv", "host", strValue) && strValue.length()) {
     host.m_strMasterHost = strValue;
  }else {
    CwxCommon::snprintf(m_szErr2K, 2047,
        "recv:host empty, data:%s", strValue.c_str());
    return -1;
  }
  ///get recv:port
  if (parse.getAttr("recv", "port", strValue) && strValue.length()) {
    host.m_unMasterPort = strtoul(strValue.c_str(), NULL, 10);
  } else {
    CwxCommon::snprintf(m_szErr2K, 2047,
        "recv port empty, data:%s",strValue.c_str());
    return -1;
  }
  return 0;
}

void CwxProProxyZkHandler::watcherMqGroup(zhandle_t* ,
    int ,
    int ,
    const char* path,
    void* context) {
  CwxProProxyZkHandler* zkHandler = (CwxProProxyZkHandler*)context;
  char *node = new char[strlen(path) + 1];
  strcpy(node, path);
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(&node));
  memcpy(msg->wr_ptr(), &node, sizeof(&node));
  msg->wr_ptr(sizeof(&node));
  msg->event().setEvent(EVENT_ZK_GROUP_EVENT);
  zkHandler->m_pApp->getZkThreadPool()->append(msg);
}

void CwxProProxyZkHandler::watherMasterMq(zhandle_t* ,
      int ,
      int ,
      const char* path,
      void* context) {
  CwxProProxyZkHandler* zkHandler = (CwxProProxyZkHandler*)context;
  char *node = new char[strlen(path) + 1];
  strcpy(node, path);
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(&node));
  memcpy(msg->wr_ptr(), &node, sizeof(&node));
  msg->wr_ptr(sizeof(&node));
  msg->event().setEvent(EVENT_ZK_MQ_EVENT);
  zkHandler->m_pApp->getZkThreadPool()->append(msg);
}
void CwxProProxyZkHandler::watcherMqTopic(zhandle_t* ,
      int ,
      int ,
      const char* path,
      void* context) {
  CwxProProxyZkHandler* zkHandler = (CwxProProxyZkHandler*)context;
  char *node = new char[strlen(path) + 1];
  strcpy(node, path);
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(&node));
  memcpy(msg->wr_ptr(), &node, sizeof(&node));
  msg->wr_ptr(sizeof(&node));
  msg->event().setEvent(EVENT_ZK_TOPIC_CHANGE);
  zkHandler->m_pApp->getZkThreadPool()->append(msg);
}

int CwxProProxyZkHandler::_dealMqGroupEvent(CwxTss* ,
      CwxMsgBlock*& msg,
      CWX_UINT32 ) {
  char* node;
  memcpy(&node, msg->rd_ptr(), sizeof(&node));
  string strNode = node;
  delete node;
  list<string> childs;
  int ret = m_zk->wgetNodeChildren(strNode, childs, CwxProProxyZkHandler::watcherMqGroup, this);
  if (-1 == ret) {
    CWX_ERROR(("Failure to get group-node:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
    m_group.clear();
    return -1;
  }else if((0 == ret) || (childs.size() == 0)){ ///根节点被删除了
    CWX_ERROR(("Group-node:%s no exists, clear all group.", strNode.c_str()));
    m_group.clear();
    m_topic.clear();
    return 0;
  }
  for(map<string, CwxMqGroup>::iterator iter = m_group.begin(); iter != m_group.end();) {
    if (find(childs.begin(), childs.end(), iter->first) == childs.end()) {
      ///删除该group对应的topic
      for(map<string/*topic*/, CwxMqTopicGroup >::iterator topic_iter = m_topic.begin();
          topic_iter != m_topic.end();) {
        if (topic_iter->second.eraseGroup(iter->first) == 0){ ///该topic对应group为空
          m_topic.erase(topic_iter++);
        }else {
          topic_iter++;
        }
      }
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
    CwxMqGroup group;
    group.m_strGroup = *iter;
    do{
      uiDataLen = MAX_ZK_DATA_SIZE;
      strNode = m_strZkRoot + "/" + group.m_strGroup + "/master";
      ret = m_zk->wgetNodeData(strNode, m_szZkDataBuf, uiDataLen, stat,
          CwxProProxyZkHandler::watherMasterMq, this);
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
        ///获取节点topic信息
        ///获取该group的toic节点信息
        strNode = m_strZkRoot + "/" + group.m_strGroup + "/topic";
        list<string> topic;
        ret = m_zk->wgetNodeChildren(strNode, topic,
            CwxProProxyZkHandler::watcherMqTopic, this);
        if (ret == 0) {
          CWX_ERROR(("Node:%s not exist.", strNode.c_str()));
          break;
        }else if (-1 == ret) {
          CWX_ERROR(("Failure to get node:%s child list, err:%s", strNode.c_str(), m_zk->getErrMsg()));
          return -1;
        }
        if (topic.size()) {
          for(list<string>::iterator titer = topic.begin(); titer != topic.end(); titer++) {
            map<string/*topic*/, CwxMqTopicGroup >::iterator tp = m_topic.find(*titer);
           if (tp == m_topic.end()) {
             m_topic.insert(pair<string, CwxMqTopicGroup>(*titer, CwxMqTopicGroup(*titer)));
             tp = m_topic.find(*titer);
           }
           tp->second.addGroup(group.m_strGroup);
          }
        }
        group.setWathTopic();
      }
    }while(0);
    m_group[group.m_strGroup] = group;
  }
  ///发送消息通知给recv线程
  noticeMasterMq();
  return 0;
}

int CwxProProxyZkHandler::_dealMasterMqEvent(CwxTss* ,
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
  CwxMqGroup group;
  iter = items.begin(); iter++; iter++;
  group.m_strGroup = *iter;
  CWX_UINT32 uiDataLen = MAX_ZK_DATA_SIZE;
  struct Stat stat;
  int ret = m_zk->wgetNodeData(strNode, m_szZkDataBuf, uiDataLen, stat,
      CwxProProxyZkHandler::watherMasterMq, this);
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
      group.m_strGroup.c_str(), group.m_strMasterId.c_str(), group.m_strMasterHost.c_str(),
      group.m_unMasterPort));
  ////发送变更消息给主线程
  noticeMasterMq();
  return 0;
}

int CwxProProxyZkHandler::_dealTopicEvent(CwxTss* ,
      CwxMsgBlock*& msg,
      CWX_UINT32 ) {
  char* node;
  memcpy(&node, msg->rd_ptr(), sizeof(&node));
  string strNode = node;
  delete node;
  list<string> items;
  list<string>::iterator iter;
  if (4 != CwxCommon::split(strNode, items, '/')) {
    CWX_ERROR(("Topic node:%s is not in format /cwx-mq/group/topic", strNode.c_str()));
    return -1;
  }
  iter = items.begin(); iter++; iter++;
  string group = *iter;
  ///清空该group对应的所有topic
  map<string/*topic*/, CwxMqTopicGroup >::iterator titer = m_topic.begin();
  while (titer != m_topic.end()) {
    if (0 == titer->second.eraseGroup(group)) {
      m_topic.erase(titer++); ///删除已空的topic
    }else{
      titer++;
    }
  }
  int ret = m_zk->wgetNodeChildren(strNode, items, CwxProProxyZkHandler::watcherMqTopic, this);
  if (-1 == ret) {
    CWX_ERROR(("Failure to get node:%s, err:%s", strNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }else if (0 == ret) { ///节点被删除
    CWX_ERROR(("Node:%s not exist.", strNode.c_str()));
    map<string/*group*/, CwxMqGroup>::iterator it = m_group.find(group);
    if (it != m_group.end()) it->second.clearWathTopic();
    return 0;
  }
  for(iter = items.begin(); iter != items.end(); iter++) {
   map<string/*topic*/, CwxMqTopicGroup >::iterator tp = m_topic.find(*iter);
   if (tp == m_topic.end()) {
     m_topic.insert(pair<string, CwxMqTopicGroup>(*iter, CwxMqTopicGroup(*iter)));
     tp = m_topic.find(*iter);
   }
   tp->second.addGroup(group);
  }
  ///发送消息到recv
  noticeTopicChange();
  return 0;
}

int CwxProProxyZkHandler::checkMqGroup() {
  if (!m_group.size()) return 0;
  int ret;
  list<string> items;
  bool bGroupChanged = false;
  bool bTopicChanged = false;
  map<string/*group*/, CwxMqGroup>::iterator iter = m_group.begin();
  for(; iter != m_group.end(); iter++) {
    if ((iter->second.isMasterEmpty())) {
      ret = checkGroup(iter->first);
      if (-1 == ret) return -1;
      if (1 == ret) bGroupChanged = true;
    }
    if (!(iter->second.isWathTopic())) {
      ret = checkGroupTopic(iter->first);
      if (-1 == ret) return -1;
      if (1 == ret) bTopicChanged = true;
    }
  }
  ///发送group变化通知
  if (bGroupChanged)
    noticeMasterMq();
  ///发送topic变化通知
  if (bTopicChanged)
    noticeTopicChange();
  return 0;
}

int CwxProProxyZkHandler::checkGroup(string strGroup) {
  CwxMqGroup group;
  group.m_strGroup = strGroup;
  group.m_bWatcherTopic = m_group[strGroup].m_bWatcherTopic;
  ///设置该group的master-mq信息
  string strNode = m_strZkRoot + "/" + strGroup + "/master";
  CWX_UINT32 uiDataLen = MAX_ZK_DATA_SIZE;
  struct Stat stat;
  CWX_INFO(("Check node:%s exist.", strNode.c_str()));
  int ret = m_zk->wgetNodeData(strNode, m_szZkDataBuf, uiDataLen, stat,
      CwxProProxyZkHandler::watherMasterMq, this);
  if (-1 == ret) {
    CWX_ERROR(("Failure to get node:%s ,err:%s", strNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }else if (0 == ret) {
    CWX_ERROR(("Node:%s not exist.", strNode.c_str()));
    return 0;
  }else { ///获取到节点信息
    m_szZkDataBuf[uiDataLen] = 0x00;
    group.m_strMasterId = m_szZkDataBuf;
    strNode = m_strZkRoot + "/" + strGroup + "/mq/" + group.m_strMasterId;
    uiDataLen = MAX_ZK_DATA_SIZE;
    ret = m_zk->getNodeData(strNode, m_szZkDataBuf, uiDataLen, stat);
    if (-1 == ret) {
      CWX_ERROR(("Failure to get node:%s , err:%s", strNode.c_str(), m_zk->getErrMsg()));
      return -1;
    }else if(0 == ret) {
      CWX_ERROR(("Node:%s not exists.", strNode.c_str()));
      return 0;
    }
    m_szZkDataBuf[uiDataLen] = 0x00;
    list<string> items;
    if (3 != CwxCommon::split(string(m_szZkDataBuf), items, ':')) {
      CWX_ERROR(("Node:%s value:%s is not in format[mq-conf:max-sid:timestamp]",
          strNode.c_str(), m_szZkDataBuf));
      return 0;
    }
    if (0 != _parseMqConf(*(items.begin()), group)){
      CWX_ERROR(("Node:%s, value:%s invalid config", strNode.c_str(), m_szZkDataBuf));
      return 0;
    }
    m_group[group.m_strGroup] = group;
    CWX_INFO(("Check valid group:%s, master-id:%s, master-host:%s, master-port:%d",
        group.m_strGroup.c_str(), group.m_strMasterId.c_str(),
        group.m_strMasterHost.c_str(), group.m_unMasterPort));
  }
  return 1;
}

int CwxProProxyZkHandler::checkGroupTopic(string group) {
  ///清空该group对应的topic信息
  map<string/*topic*/, CwxMqTopicGroup >::iterator iter = m_topic.begin();
  while (iter != m_topic.end()) {
    if (0 == iter->second.eraseGroup(group)){
      m_topic.erase(iter++);
    }else {
      iter++;
    }
  }
  string strNode = m_strZkRoot + "/" + group + "/topic";
  CWX_INFO(("Check node:%s exist.", strNode.c_str()));
  list<string> childs;
  int ret = m_zk->wgetNodeChildren(strNode, childs, CwxProProxyZkHandler::watcherMqTopic, this);
  if (ret == 0) {
    CWX_ERROR(("Topic node:%s not exist.", strNode.c_str()));
    return 0;
  }else if (-1 == ret) {
    CWX_ERROR(("Failure to get node:%s child, err:%s", strNode.c_str(), m_zk->getErrMsg()));
    return -1;
  }
  m_group[group].setWathTopic();
  if (childs.size()) {
    for(list<string>::iterator item = childs.begin(); item != childs.end(); item++) {
      map<string/*topic*/, CwxMqTopicGroup >::iterator tp = m_topic.find(*item);
     if (tp == m_topic.end()) {
       m_topic.insert(pair<string, CwxMqTopicGroup>(*item, CwxMqTopicGroup(*item)));
       tp = m_topic.find(*item);
     }
     tp->second.addGroup(group);
    }
  }
  return 1;
}

void CwxProProxyZkHandler::noticeMasterMq() {
  map<string/*group*/, CwxMqGroup>::iterator iter = m_group.begin();
  map<string, CwxMqGroup>* group = new map<string, CwxMqGroup>;
  while( iter != m_group.end()) {
    if (!(iter->second.isMasterEmpty())) group->insert(pair<string, CwxMqGroup>(iter->first, iter->second));
    iter++;
  }
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(&group));
  memcpy(msg->wr_ptr(), &group, sizeof(&group));
  msg->wr_ptr(sizeof(&group));
  msg->event().setSvrId(CwxProProxyApp::SVR_PRO_TYPE_RECV);
  msg->event().setEvent(EVENT_ZK_MASTER_EVENT);
  m_pApp->getMqThreadPool()->append(msg);
}

void CwxProProxyZkHandler::noticeTopicChange() {
  map<string/*group*/, CwxMqTopicGroup >::iterator iter = m_topic.begin();
  map<string/*group*/, CwxMqTopicGroup >* topic = new map<string, CwxMqTopicGroup>;
  while (iter != m_topic.end()) {
    topic->insert(pair<string, CwxMqTopicGroup>(iter->first, iter->second));
    iter++;
  }
  CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(&topic));
  memcpy(msg->wr_ptr(), &topic, sizeof(&topic));
  msg->wr_ptr(sizeof(&topic));
  msg->event().setSvrId(CwxProProxyApp::SVR_PRO_TYPE_RECV);
  msg->event().setEvent(EVENT_ZK_TOPIC_CHANGE);
  m_pApp->getMqThreadPool()->append(msg);
}
