#include "CwxMqOuterDispHandler.h"
#include "CwxMqApp.h"

// session的map，key为session id
map<CWX_UINT64, CwxMqOuterDispSession*> CwxMqOuterDispHandler::m_sessions;
///<需要关闭的session
list<CwxMqOuterDispSession*> CwxMqOuterDispHandler::m_freeSession;

///添加一个新连接
void CwxMqOuterDispSession::addConn(CwxMqOuterDispHandler* conn) {
  CWX_ASSERT(m_conns.find(conn->getConnId()) == m_conns.end());
  m_conns[conn->getConnId()] = conn;
}

CwxMqOuterDispSession::~CwxMqOuterDispSession() {
  if (m_sourceFile) {
    m_sourceFile->syncFile();
    delete m_sourceFile;
  }
  if (CwxMqConfigCmn::MQ_TYPE_ZK == m_pApp->getConfig().getCommon().m_type &&
      m_strSource.length() && m_ullSid) {
    CwxMqZkSource source(m_strSource, m_ullSid, m_ullLeftBinlogNum);
    m_pApp->updateSource(m_strTopic, source);
  }
  if (m_pCursor) {
   m_binlogMgr->destoryCurser(m_pCursor);
  }
  m_pApp->getTopicMgr()->freeBinlogMgrByTopic(m_strTopic, m_binlogMgr);
}

///构造函数
CwxMqOuterDispHandler::CwxMqOuterDispHandler(CwxMqApp* pApp,
                                   CwxAppChannel* channel,
                                   CWX_UINT32 uiConnId) :CwxAppHandler4Channel(channel)
{
  m_bReport = false;
  m_uiConnId = uiConnId;
  m_ullSentSeq = 0;
  m_ullLastSid = 0;
  m_syncSession = NULL;
  m_pApp = pApp;
  m_uiRecvHeadLen = 0;
  m_uiRecvDataLen = 0;
  m_recvMsgData = 0;
  m_ullSessionId = 0;
  m_tss = NULL;
}

///析构函数
CwxMqOuterDispHandler::~CwxMqOuterDispHandler() {
  if (m_recvMsgData)
    CwxMsgBlockAlloc::free(m_recvMsgData);
  m_recvMsgData = NULL;
}

///释放资源
void CwxMqOuterDispHandler::destroy(CwxMqApp*) {
  {
    map<CWX_UINT64, CwxMqOuterDispSession*>::iterator iter = m_sessions.begin();
    while (iter != m_sessions.end()) {
      delete iter->second;
      iter++;
    }
    m_sessions.clear();
  }
}

void CwxMqOuterDispHandler::doEvent(CwxMqApp* app,
                               CwxMqTss* tss,
                               CwxMsgBlock*& msg)
{
  if (CwxEventInfo::CONN_CREATED == msg->event().getEvent()) { ///连接建立
    CwxAppChannel* channel = app->getOuterDispChannel();
    if (channel->isRegIoHandle(msg->event().getIoHandle())) {
      CWX_ERROR(("Handler[%d] is register, it's a big bug. exit....", msg->event().getIoHandle()));
      app->stop();
      return;
    }
    CwxMqOuterDispHandler* pHandler = new CwxMqOuterDispHandler(app, channel,
      app->reactor()->getNextConnId());
    ///获取连接的来源信息
    CwxINetAddr remoteAddr;
    CwxSockStream stream(msg->event().getIoHandle());
    stream.getRemoteAddr(remoteAddr);
    pHandler->m_unPeerPort = remoteAddr.getPort();
    if (remoteAddr.getHostIp(tss->m_szBuf2K, 2047)) {
      pHandler->m_strPeerHost = tss->m_szBuf2K;
    }
    ///设置handle的io后，open handler
    pHandler->setHandle(msg->event().getIoHandle());
    if (0 != pHandler->open()) {
      CWX_ERROR(("Failure to register sync handler[%d], from:%s:%u", pHandler->getHandle(), pHandler->m_strPeerHost.c_str(), pHandler->m_unPeerPort));
      delete pHandler;
      return;
    }
    ///设置对象的tss对象
    pHandler->m_tss = (CwxMqTss*) CwxTss::instance();
    CWX_INFO(("Accept sync connection from %s:%u", pHandler->m_strPeerHost.c_str(), pHandler->m_unPeerPort));
  } else{
    CWX_ASSERT(msg->event().getEvent() == CwxEventInfo::TIMEOUT_CHECK);
    CWX_ASSERT(msg->event().getSvrId() == CwxMqApp::SVR_TYPE_OUTER_DISP);
    map<CWX_UINT64, CwxMqOuterDispSession*>::iterator iter = m_sessions.begin();
    if (CwxMqConfigCmn::MQ_TYPE_ZK == app->getConfig().getCommon().m_type) {
      while (iter != m_sessions.end()) {
        if (iter->second->m_strSource.length() && iter->second->m_ullSid ) {
          CwxMqZkSource source(iter->second->m_strSource, iter->second->m_ullSid, iter->second->m_ullLeftBinlogNum);
          app->updateSource(iter->second->m_strTopic, source);
        }
        ++iter;
      }
    }else{
      while (iter != m_sessions.end()) {
        if (iter->second->m_sourceFile)
          iter->second->m_sourceFile->timeout(app->getCurTime());
        ++iter;
      }
    }
  }
}

///释放关闭的session
void CwxMqOuterDispHandler::dealClosedSession(CwxMqApp* , CwxMqTss*) {
  list<CwxMqOuterDispSession*>::iterator iter;
  CwxMqOuterDispHandler* handler;
  ///获取用户object对象
  if (m_freeSession.begin() != m_freeSession.end()) {
    iter = m_freeSession.begin();
    while (iter != m_freeSession.end()) {
      ///session必须是closed状态
      CWX_ASSERT((*iter)->m_bClosed);
      CWX_INFO(("Close sync session from host:%s", (*iter)->m_strHost.c_str()));
      ///将session从session的map中删除
      m_sessions.erase((*iter)->m_ullSessionId);
      ///开始关闭连接
      map<CWX_UINT32, CwxMqOuterDispHandler*>::iterator conn_iter = (*iter)->m_conns.begin();
      while (conn_iter != (*iter)->m_conns.end()) {
        handler = conn_iter->second;
        (*iter)->m_conns.erase(handler->getConnId());
        handler->close(); ///此为同步调用
        conn_iter = (*iter)->m_conns.begin();
      }
      delete *iter;
      iter++;
    }
    m_freeSession.clear();
  }
}

/**
@brief 连接可读事件，返回-1，close()会被调用
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqOuterDispHandler::onInput() {
  ///接受消息
  int ret = CwxAppHandler4Channel::recvPackage(getHandle(), m_uiRecvHeadLen,
    m_uiRecvDataLen, m_szHeadBuf, m_header, m_recvMsgData);
  ///如果没有接受完毕（0）或失败（-1），则返回
  if (1 != ret) return ret;
  ///接收到一个完整的数据包，消息处理
  ret = recvMessage();
  ///如果没有释放接收的数据包，释放
  if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
  this->m_recvMsgData = NULL;
  this->m_uiRecvHeadLen = 0;
  this->m_uiRecvDataLen = 0;
  return ret;
}

//1：不从engine中移除注册；0：从engine中移除注册但不删除handler；-1：从engine中将handle移除并删除。
int CwxMqOuterDispHandler::onConnClosed() {
  ///一条连接关闭，则整个session失效
  if (m_syncSession) {
    ///如果连接对应的session存在
    if (m_sessions.find(m_ullSessionId) != m_sessions.end()) {
      if (!m_syncSession->m_bClosed) {
        ///将session标记为close
        m_syncSession->m_bClosed = true;
        ///将session放到需要是否的session列表
        m_freeSession.push_back(m_syncSession);
      }
      ///将连接从session的连接中删除，因此此连接将被delete
      m_syncSession->m_conns.erase(m_uiConnId);
    }
  }
  return -1;
}

///收到消息
int CwxMqOuterDispHandler::recvMessage() {
  if (CwxMqPoco::MSG_TYPE_SYNC_OUTER_REPORT == m_header.getMsgType()) {
    return recvReport(m_tss);
  } else if (CwxMqPoco::MSG_TYPE_SYNC_SESSION_REPORT == m_header.getMsgType()) {
    return recvNewConnection(m_tss);
  } else if (CwxMqPoco::MSG_TYPE_SYNC_DATA_REPLY == m_header.getMsgType()) {
    return recvReply(m_tss);
  } else if (CwxMqPoco::MSG_TYPE_SYNC_DATA_CHUNK_REPLY == m_header.getMsgType()) {
      return recvReply(m_tss);
  }
  ///直接关闭连接
  CWX_ERROR(("Recv invalid msg type:%u from host:%s:%u, close connection.", m_header.getMsgType(), m_strPeerHost.c_str(), m_unPeerPort));
  return -1;
}

int CwxMqOuterDispHandler::recvReport(CwxMqTss* pTss) {
  int iRet = 0;
  CWX_UINT64 ullSid = 0;
  bool bNewly = false;
  CWX_UINT32 uiChunk = 0;
  char const* topic = NULL;
  char const* source = NULL;
  bool bzip = false;
  CwxMsgBlock* msg = NULL;
  ///CWX_INFO(("Recv report from host:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
  do {
    if (!m_recvMsgData) {
      strcpy(pTss->m_szBuf2K, "No data.");
      CWX_ERROR(("Report package is empty, from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
    ///禁止重复report sid。若cursor存在，表示已经报告过一次
    if (m_syncSession) {
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
        "Can't report sync sid duplicate.");
      CWX_ERROR(("Report is duplicate, from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    ///若是同步sid的报告消息,则获取报告的sid
    iRet = CwxMqPoco::parseOuterReportData(pTss->m_pReader,
      m_recvMsgData,
      ullSid,
      topic,
      bNewly,
      uiChunk,
      source,
      bzip,
      pTss->m_szBuf2K);
    if (CWX_MQ_ERR_SUCCESS != iRet) {
      CWX_ERROR(("Failure to pack report msg, err=%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    CWX_INFO(("Recv report from:%s:%u, sid=%s, topic=%s, from_new=%s, chunk=%u, source=%s, zip=%s", m_strPeerHost.c_str(), m_unPeerPort, CwxCommon::toString(ullSid, pTss->m_szBuf2K, 10), topic, bNewly?"yes":"no", uiChunk, source?source:"", bzip?"yes":"no"));
    CwxBinLogMgr* binlogMgr = m_pApp->getTopicMgr()->getBinlogMgrByTopic(topic);
    ///判断topic的合法性
    if(!binlogMgr || (binlogMgr->getZkTopicState() == CWX_MQ_TOPIC_DELETE)) {
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
          "Invalid topic[%s]", topic);
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      if (binlogMgr) m_pApp->getTopicMgr()->freeBinlogMgrByTopic(topic, binlogMgr);
      break;
    }
    m_syncSession = new CwxMqOuterDispSession(m_pApp, binlogMgr);
    m_syncSession->m_strHost = m_strPeerHost;
    m_syncSession->m_uiChunk = uiChunk;
    m_syncSession->m_bZip = bzip;
    m_syncSession->m_strTopic = topic;
    if (m_syncSession->m_uiChunk) {
      if (m_syncSession->m_uiChunk > CwxMqConfigCmn::MAX_CHUNK_SIZE_KB)
        m_syncSession->m_uiChunk = CwxMqConfigCmn::MAX_CHUNK_SIZE_KB;
      if (m_syncSession->m_uiChunk < CwxMqConfigCmn::MIN_CHUNK_SIZE_KB)
        m_syncSession->m_uiChunk = CwxMqConfigCmn::MIN_CHUNK_SIZE_KB;
      m_syncSession->m_uiChunk *= 1024;
    }
    if (bNewly) { ///不sid为空，则取当前最大sid-1
      ullSid = binlogMgr->getMaxSid();
      if (ullSid) ullSid--;
    }
    if (source && strlen(source)) { ///检查source是否存在
      map<CWX_UINT64, CwxMqOuterDispSession*>::iterator iter =  m_sessions.begin();
      while (iter != m_sessions.end()) {
        if (iter->second->m_strSource == source) {
          iRet = CWX_MQ_ERR_ERROR;
          CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
            "Source[%s]'s connection exist, from[%s]", source,
            iter->second->m_strHost.c_str());
          CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
          break;
        }
        ++iter;
      }
      ///如果中间有失败，则退出
      if (CwxMqConfigCmn::MQ_TYPE_ZK == m_pApp->getConfig().getCommon().m_type) {
        m_syncSession->m_strSource = source;
      }else {
        m_syncSession->m_strSource = m_pApp->getConfig().getOuterDispatch().m_strSourcePath + source;
      }
      if (iter != m_sessions.end()) break;
      if (CwxMqConfigCmn::MQ_TYPE_ZK == m_pApp->getConfig().getCommon().m_type) { ///zk模式
        CWX_UINT64 sid = 0;
        iRet = m_pApp->getSouce(topic, source, sid);
        if (1 == iRet) {
          //如果没有指定ullSid,则采用source的值
          if (!bNewly && (0 == ullSid))
            ullSid = sid;
          bNewly = false;
        }/*else {
          ///CwxMqZkSource zkSource(source, ullSid, 0);
          ////m_pApp->updateSource(topic, zkSource);
        }*/
      } else { ///单机模式
        m_syncSession->m_sourceFile = new CwxSidLogFile(
          m_pApp->getConfig().getOuterDispatch().m_uiFlushNum,
          m_pApp->getConfig().getOuterDispatch().m_uiFlushSecond,
          m_syncSession->m_strSource);
        iRet = m_syncSession->m_sourceFile->load();
        if (-1 == iRet) {
          iRet = CWX_MQ_ERR_ERROR;
          CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
            "Failure to load source file[%s], err=%s", m_syncSession->m_strSource.c_str(),
            m_syncSession->m_sourceFile->getErrMsg());
          CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
          break;
        }else if (0 == iRet){
          if (0 != m_syncSession->m_sourceFile->create(
              m_syncSession->m_strSource,
            ullSid,
            "",
            ""))
          {
            iRet = CWX_MQ_ERR_ERROR;
            CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
              "Failure to create source file[%s], err=%s", m_syncSession->m_strSource.c_str(),
              m_syncSession->m_sourceFile->getErrMsg());
            CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
            break;
          }
        }else{
          //如果没有指定ullSid，则采用source的值
          if (!bNewly && (0 == ullSid)){
            ullSid = m_syncSession->m_sourceFile->getCurMaxSid();
          }
          bNewly = false;
        }
      }
    }
    CWX_INFO(("Recv report from:%s:%u, start_sid=%s",
        m_strPeerHost.c_str(), m_unPeerPort, CwxCommon::toString(ullSid, pTss->m_szBuf2K, 10)));
    m_syncSession->reformSessionId();
    ///将session加入到session的map
    while (m_sessions.find(m_syncSession->m_ullSessionId) != m_sessions.end()) {
      m_syncSession->reformSessionId();
    }
    m_sessions[m_syncSession->m_ullSessionId] = m_syncSession;
    m_ullSessionId = m_syncSession->m_ullSessionId;
    m_syncSession->m_ullSid = ullSid;
    m_syncSession->m_ullStartSid = ullSid;
    m_syncSession->addConn(this);
    ///回复iRet的值
    iRet = CWX_MQ_ERR_SUCCESS;
    ///创建binlog读取的cursor
    CwxBinLogCursor* pCursor = m_syncSession->m_binlogMgr->createCurser(ullSid); ///m_pApp->getBinLogMgr()->createCurser(ullSid);
    if (!pCursor) {
      iRet = CWX_MQ_ERR_ERROR;
      strcpy(pTss->m_szBuf2K, "Failure to create cursor");
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    pCursor->setSeekSid(ullSid);
    pCursor->setSeekState(CwxBinLogCursor::CURSOR_STATE_UNSEEK);
    if (!bNewly && !source) {
      if (ullSid && ullSid < m_syncSession->m_binlogMgr->getMinSid()) {
        ///m_pApp->getBinLogMgr()->destoryCurser(pCursor);
        m_syncSession->m_binlogMgr->destoryCurser(pCursor);
        delete pCursor;
        pCursor = NULL;
        iRet = CWX_MQ_ERR_LOST_SYNC;
        char szBuf1[64], szBuf2[64];
        sprintf(pTss->m_szBuf2K, "Lost sync state, report sid:%s, min sid:%s",
          CwxCommon::toString(ullSid, szBuf1),
          CwxCommon::toString(m_syncSession->m_binlogMgr->getMinSid(), szBuf2));
        CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
        break;
      }
    }
    ///设置cursor
    m_syncSession->m_pCursor = pCursor;

    ///发送session id的消息
    if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packReportDataReply(pTss->m_pWriter,
      msg,
      m_header.getTaskId(),
      m_syncSession->m_ullSessionId,
      pTss->m_szBuf2K))
    {
      CWX_ERROR(("Failure to pack sync data reply, err=%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      return -1;
    }
    msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    if (!putMsg(msg)) {
      CwxMsgBlockAlloc::free(msg);
      CWX_ERROR(("Failure push msg to send queue. from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      return -1;
    }
    ///发送下一条binlog
    int iState = syncSendBinLog(pTss);
    if (-1 == iState) {
      iRet = CWX_MQ_ERR_ERROR;
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    } else if (0 == iState) { ///产生continue的消息
      channel()->regRedoHander(this);
    }
    return 0;
  } while (0);
  ///到此一定错误
  CWX_ASSERT(CWX_MQ_ERR_SUCCESS != iRet);
  CwxMsgBlock* pBlock = NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncErr(pTss->m_pWriter,
    pBlock,
    m_header.getTaskId(),
    iRet,
    pTss->m_szBuf2K,
    pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to create binlog reply package, err:%s", pTss->m_szBuf2K));
    return -1;
  }
  pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
  if (!putMsg(pBlock)) {
    CwxMsgBlockAlloc::free(pBlock);
    CWX_ERROR(("Failure push msg to send queue. from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
    return -1;
  }
  return 0;
}

int CwxMqOuterDispHandler::recvNewConnection(CwxMqTss* pTss) {
  int iRet = 0;
  CWX_UINT64 ullSession = 0;
  CwxMsgBlock* msg = NULL;
  do {
    if (!m_recvMsgData) {
      strcpy(pTss->m_szBuf2K, "No data.");
      CWX_ERROR(("Session connect-report package is empyt, from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
    ///禁止重复report sid。若cursor存在，表示已经报告过一次
    if (m_syncSession) {
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
        "Can't report sync sid duplicatly.");
      CWX_ERROR(("Session connect-report is duplicate, from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    ///获取报告的session id
    iRet = CwxMqPoco::parseReportNewConn(pTss->m_pReader,
      m_recvMsgData,
      ullSession,
      pTss->m_szBuf2K);
    if (CWX_MQ_ERR_SUCCESS != iRet) {
      CWX_ERROR(("Failure to parse report new conn msg, err=%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    if (m_sessions.find(ullSession) == m_sessions.end()) {
      iRet = CWX_MQ_ERR_ERROR;
      char szTmp[64];
      CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Session[%s] doesn't exist",
        CwxCommon::toString(ullSession, szTmp, 10));
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    m_syncSession = m_sessions.find(ullSession)->second;
    m_ullSessionId = m_syncSession->m_ullSessionId;
    m_syncSession->addConn(this);
    ///发送下一条binlog
    int iState = syncSendBinLog(pTss);
    if (-1 == iState) {
      iRet = CWX_MQ_ERR_ERROR;
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    } else if (0 == iState) { ///产生continue的消息
      channel()->regRedoHander(this);
    }
    return 0;
  } while (0);
  ///到此一定错误
  CWX_ASSERT(CWX_MQ_ERR_SUCCESS != iRet);
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncErr(pTss->m_pWriter,
    msg,
    m_header.getTaskId(),
    iRet,
    pTss->m_szBuf2K,
    pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to pack sync data reply, err=%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
    return -1;
  }
  msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
  if (!putMsg(msg)) {
    CwxMsgBlockAlloc::free(msg);
    CWX_ERROR(("Failure push msg to send queue. from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
    return -1;
  }
  return 0;
}

int CwxMqOuterDispHandler::recvReply(CwxMqTss* pTss) {
  int iRet = CWX_MQ_ERR_SUCCESS;
  CWX_UINT64 ullSeq = 0;
  CwxMsgBlock* msg = NULL;
  do {
    if (!m_syncSession) { ///如果连接不是同步状态，则是错误
      strcpy(pTss->m_szBuf2K, "Client no in sync state");
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
    if (!m_recvMsgData) {
      strcpy(pTss->m_szBuf2K, "No data.");
      CWX_ERROR(("Sync reply package is empty, from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
    ///若是同步sid的报告消息,则获取报告的sid
    iRet = CwxMqPoco::parseSyncDataReply(pTss->m_pReader, m_recvMsgData, ullSeq,
      pTss->m_szBuf2K);
    if (CWX_MQ_ERR_SUCCESS != iRet) {
      CWX_ERROR(("Failure to parse sync_data reply package, err:%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    if (ullSeq != m_ullSentSeq) {
      char szTmp1[64];
      char szTmp2[64];
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
        "Seq[%s] is not same with the connection's[%s].",
        CwxCommon::toString(ullSeq, szTmp1, 10),
        CwxCommon::toString(m_ullSentSeq, szTmp2, 10));
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    if (m_syncSession->m_sourceFile){
      if (-1 == m_syncSession->m_sourceFile->log(m_ullLastSid)){
        iRet = CWX_MQ_ERR_ERROR;
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
          "Falure to write source log file , err=%s", m_syncSession->m_sourceFile->getErrMsg());
        CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
        break;
      }
    }else {
      m_syncSession->m_ullSid = m_ullLastSid;
      ///CWX_INFO(("Session innsert: set out dispatch session sid:%u, left:%d",
          ///m_ullLastSid, m_syncSession->m_ullLeftBinlogNum));
    }
    ///发送下一条binlog
    int iState = syncSendBinLog(pTss);
    if (-1 == iState) {
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      return -1; ///关闭连接
    } else if (0 == iState) { ///产生continue的消息
      channel()->regRedoHander(this);
    }
    return 0;
  } while (0);
  ///到此一定错误
  CWX_ASSERT(CWX_MQ_ERR_SUCCESS != iRet);
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncErr(pTss->m_pWriter,
    msg,
    m_header.getTaskId(),
    iRet,
    pTss->m_szBuf2K,
    pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to pack sync data reply, err=%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
    return -1;
  }
  msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
  if (!putMsg(msg)) {
    CwxMsgBlockAlloc::free(msg);
    CWX_ERROR(("Failure push msg to send queue. from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
    return -1;
  }
  return 0;
}

/**
@brief Handler的redo事件，在每次dispatch时执行。
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxMqOuterDispHandler::onRedo() {
  ///判断是否有可发送的消息
  if (m_syncSession->m_ullSid < m_syncSession->m_binlogMgr->getMaxSid()) {
    ///发送下一条binlog
    int iState = syncSendBinLog(m_tss);
    if (-1 == iState) {
      CwxMsgBlock* msg = NULL;
      if (CWX_MQ_ERR_ERROR != CwxMqPoco::packSyncErr(m_tss->m_pWriter,
        msg,
        m_header.getTaskId(),
        CWX_MQ_ERR_ERROR,
        m_tss->m_szBuf2K,
        m_tss->m_szBuf2K))
      {
        CWX_ERROR(("Failure to pack sync data reply, err=%s, from:%s:%u", m_tss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
        return -1;
      }
      msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
      if (!putMsg(msg)) {
        CwxMsgBlockAlloc::free(msg);
        CWX_ERROR(("Failure push msg to send queue. from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
        return -1;
      }
    } else if (0 == iState) { ///产生continue的消息
      channel()->regRedoHander(this);
    }
  } else {
    ///重新注册
    channel()->regRedoHander(this);
  }
  ///返回
  return 0;
}

///0：未发送一条binlog；
///1：发送了一条binlog；
///-1：失败；
int CwxMqOuterDispHandler::syncSendBinLog(CwxMqTss* pTss) {
  int iRet = 0;
  CwxMsgBlock* pBlock = NULL;
  CWX_UINT32 uiKeyLen = 0;
  CWX_UINT32 uiTotalLen = 0;
  CWX_UINT64 ullSeq = m_syncSession->m_ullSeq;
  if (m_syncSession->m_binlogMgr->getZkTopicState() == CWX_MQ_TOPIC_DELETE) {
    CWX_INFO(("Topic:%s is deleted, need close out dispatch.", m_syncSession->m_strTopic.c_str()));
    return -1;
  }
  if (m_syncSession->m_pCursor->isUnseek()) { //若binlog的读取cursor悬空，则定位
    if (1 != (iRet = syncSeekToReportSid(pTss))) return iRet;
  }

  if (m_syncSession->m_uiChunk) pTss->m_pWriter->beginPack();
  while (1) {
    if (1 != (iRet = syncSeekToBinlog(pTss))) break;
    //设置移到下一个记录位置
    m_syncSession->m_bNext = true;
    if (!m_syncSession->m_uiChunk) {
      iRet = syncPackOneBinLog(pTss->m_pWriter,
        pBlock,
        ullSeq,
        pTss->m_pBinlogData,
        pTss->m_szBuf2K);
      m_ullLastSid = m_syncSession->m_pCursor->getHeader().getSid();
      m_syncSession->m_ullLeftBinlogNum = m_syncSession->m_binlogMgr->leftLogNum(m_syncSession->m_pCursor);
      break;
    } else {
      iRet = syncPackMultiBinLog(pTss->m_pWriter,
        pTss->m_pItemWriter,
        pTss->m_pBinlogData,
        uiKeyLen,
        pTss->m_szBuf2K);
      m_ullLastSid = m_syncSession->m_pCursor->getHeader().getSid();
      m_syncSession->m_ullLeftBinlogNum = m_syncSession->m_binlogMgr->leftLogNum(m_syncSession->m_pCursor);
      if (1 == iRet) {
        uiTotalLen += uiKeyLen;
        if (uiTotalLen >= m_syncSession->m_uiChunk) break;
      }
      if (-1 == iRet) break;
      continue;
    }
  }

  if (-1 == iRet) return -1;
  if (!m_syncSession->m_uiChunk) { ///若不是chunk
    if (0 == iRet) return 0; ///没有数据
  } else {
    if (0 == uiTotalLen) return 0;
    pTss->m_pWriter->pack();
    if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packMultiSyncData(0, pTss->m_pWriter->getMsg(),
      pTss->m_pWriter->getMsgSize(),
      pBlock,
      ullSeq,
      m_syncSession->m_bZip,
      pTss->m_szBuf2K))
    {
      return -1;
    }
  }
  ///根据svr类型，发送数据包
  pBlock->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
  pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_OUTER_DISP);
  pBlock->send_ctrl().setHostId(0);
  pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (!putMsg(pBlock)) {
    CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Failure to send binlog");
    CWX_ERROR((pTss->m_szBuf2K));
    CwxMsgBlockAlloc::free(pBlock);
    return -1;
  }
  m_ullSentSeq = ullSeq;
  m_syncSession->m_ullSeq++;
  return 1; ///发送了一条消息
}

//1：成功；0：太大；-1：错误
int CwxMqOuterDispHandler::syncSeekToReportSid(CwxMqTss* tss) {
  int iRet = 0;
  if (m_syncSession->m_pCursor->isUnseek()) { //若binlog的读取cursor悬空，则定位
    if (m_syncSession->m_ullStartSid < m_syncSession->m_binlogMgr->getMaxSid()) {
      iRet = m_syncSession->m_binlogMgr->seek(m_syncSession->m_pCursor,
        m_syncSession->m_ullStartSid);
      if (-1 == iRet) {
        CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Failure to seek,  err:%s",
          m_syncSession->m_pCursor->getErrMsg());
        CWX_ERROR((tss->m_szBuf2K));
        return -1;
      } else if (0 == iRet) {
        char szBuf1[64];
        char szBuf2[64];
        CwxCommon::snprintf(tss->m_szBuf2K, 2047,
          "Should seek to sid[%s] with max_sid[[%s], but not.",
          CwxCommon::toString(m_syncSession->m_ullStartSid, szBuf1),
          CwxCommon::toString(m_syncSession->m_binlogMgr->getMaxSid(), szBuf2));
        CWX_ERROR((tss->m_szBuf2K));
        return 0;
      }
      ///若成功定位，则读取当前记录
      m_syncSession->m_bNext = m_syncSession->m_ullStartSid == m_syncSession->m_pCursor->getHeader().getSid() ? true : false;
    } else { ///若需要同步发送的sid不小于当前最小的sid，则依旧为悬空状态
      return 0; ///完成状态
    }
  }
  return 1;
}

///-1：失败，1：成功
int CwxMqOuterDispHandler::syncPackOneBinLog(CwxPackageWriter* writer,
                                        CwxMsgBlock*& block,
                                        CWX_UINT64 ullSeq,
                                        CwxKeyValueItem const* pData,
                                        char* szErr2K)
{
  ///形成binlog发送的数据包
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncData(writer,
    block,
    0,
    m_syncSession->m_pCursor->getHeader().getSid(),
    m_syncSession->m_pCursor->getHeader().getDatetime(),
    m_syncSession->m_strTopic.c_str(),
    *pData,
    m_syncSession->m_bZip,
    ullSeq,
    szErr2K))
  {
    ///形成数据包失败
    CWX_ERROR(("Failure to pack binlog package, err:%s", szErr2K));
    return -1;
  }
  return 1;
}

///-1：失败，否则返回添加数据的尺寸
int CwxMqOuterDispHandler::syncPackMultiBinLog(CwxPackageWriter* writer,
                                          CwxPackageWriter* writer_item,
                                          CwxKeyValueItem const* pData,
                                          CWX_UINT32& uiLen,
                                          char* szErr2K)
{
  ///形成binlog发送的数据包
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataItem(writer_item,
    m_syncSession->m_pCursor->getHeader().getSid(),
    m_syncSession->m_strTopic.c_str(),
    m_syncSession->m_pCursor->getHeader().getDatetime(),
    *pData,
    szErr2K))
  {
    ///形成数据包失败
    CWX_ERROR(("Failure to pack binlog package, err:%s", szErr2K));
    return -1;
  }
  if (!writer->addKeyValue(CWX_MQ_M,
    writer_item->getMsg(),
    writer_item->getMsgSize(),
    true))
  {
    ///形成数据包失败
    CwxCommon::snprintf(szErr2K, 2047, "Failure to pack binlog package, err:%s",
      writer->getErrMsg());
    CWX_ERROR((szErr2K));
    return -1;
  }
  uiLen = CwxPackage::getKvLen(strlen(CWX_MQ_M), writer_item->getMsgSize());
  return 1;
}

//1：发现记录；0：没有发现；-1：错误
int CwxMqOuterDispHandler::syncSeekToBinlog(CwxMqTss* tss) {
  int iRet = 0;
  if (m_syncSession->m_bNext) {
    iRet = m_syncSession->m_binlogMgr->next(m_syncSession->m_pCursor);
    if (0 == iRet) return 0; ///完成状态
    if (-1 == iRet) { ///<失败
      CwxCommon::snprintf(tss->m_szBuf2K, 2047,
        "Failure to seek cursor, err:%s",
        m_syncSession->m_pCursor->getErrMsg());
      CWX_ERROR((tss->m_szBuf2K));
      return -1;
    }
  }
  CWX_UINT32 uiDataLen = m_syncSession->m_pCursor->getHeader().getLogLen();
  ///准备data读取的buf
  char* szData = tss->getBuf(uiDataLen);
  ///读取data
  iRet = m_syncSession->m_binlogMgr->fetch(m_syncSession->m_pCursor, szData,
    uiDataLen);
  if (-1 == iRet) { //读取失败
    CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Failure to fetch data, err:%s",
      m_syncSession->m_pCursor->getErrMsg());
    CWX_ERROR((tss->m_szBuf2K));
    return -1;
  }
  m_syncSession->m_bNext = false;
  while (1) {
    if (!tss->m_pReader->unpack(szData, uiDataLen, false, true)) {
      CWX_ERROR(("Can't unpack binlog, sid=%s", CwxCommon::toString(m_syncSession->m_pCursor->getHeader().getSid(), tss->m_szBuf2K)));
    } else {
      ///获取CWX_MQ_D的key，此为真正data数据
      tss->m_pBinlogData = tss->m_pReader->getKey(CWX_MQ_D);
      if (!tss->m_pBinlogData) {
        CWX_ERROR(("Can't find key[%s] in binlog, sid=%s", CWX_MQ_D, CwxCommon::toString(m_syncSession->m_pCursor->getHeader().getSid(), tss->m_szBuf2K)));
      } else {
        break;
      }
    }
    iRet = m_syncSession->m_binlogMgr->next(m_syncSession->m_pCursor);
    if (0 == iRet) {
      m_syncSession->m_bNext = true;
      return 0; ///完成状态
    }
    if (-1 == iRet) { ///<失败
      CwxCommon::snprintf(tss->m_szBuf2K, 2047,
        "Failure to seek cursor, err:%s",
        m_syncSession->m_pCursor->getErrMsg());
      CWX_ERROR((tss->m_szBuf2K));
      return -1;
    }
  };
  return 1;
}
