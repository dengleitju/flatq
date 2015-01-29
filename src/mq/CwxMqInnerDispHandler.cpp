#include "CwxMqInnerDispHandler.h"
#include "CwxMqApp.h"

// session的map, key为session id
map<CWX_UINT64, CwxMqInnerDispSession*> CwxMqInnerDispHandler::m_sessions;
// 需要关闭的session
list<CwxMqInnerDispSession*> CwxMqInnerDispHandler::m_freeSession;

///添加一个新连接
void CwxMqInnerDispSession::addConn(CwxMqInnerDispHandler* conn) {
  CWX_ASSERT(m_conns.find(conn->getConnId()) == m_conns.end());
  m_conns[conn->getConnId()] = conn;
}

CwxMqInnerDispSession::~CwxMqInnerDispSession() {
}

///构造函数
CwxMqInnerDispHandler::CwxMqInnerDispHandler(CwxMqApp* pApp,
                                   CwxAppChannel* channel,
                                   CWX_UINT32 uiConnId) :CwxAppHandler4Channel(channel)
{
  m_bReport = false;
  m_uiConnId = uiConnId;
  m_ullSendSeq = 0;
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
CwxMqInnerDispHandler::~CwxMqInnerDispHandler() {
  if (m_recvMsgData)
    CwxMsgBlockAlloc::free(m_recvMsgData);
  m_recvMsgData = NULL;
}

///释放资源
void CwxMqInnerDispHandler::destroy(CwxMqApp* app) {
  {
    map<CWX_UINT64, CwxMqInnerDispSession*>::iterator iter1 = m_sessions.begin();
    while (iter1 != m_sessions.end()) {
      CwxMqInnerDispSession* session = iter1->second;
      if (session->m_binlogCursor.size()) {
        map<string, CwxBinlogMgrCursor*>::iterator iter2 = session->m_binlogCursor.begin();
        while(iter2 != session->m_binlogCursor.end()) {
          iter2->second->m_binlogMgr->destoryCurser(iter2->second->m_pCursor);
          app->getTopicMgr()->freeBinlogMgrByTopic(iter2->first, iter2->second->m_binlogMgr);
          delete iter2->second;
          iter2++;
        }
        session->m_binlogCursor.clear();
      }
      delete iter1->second;
      iter1++;
    }
    m_sessions.clear();
  }
}

void CwxMqInnerDispHandler::doEvent(CwxMqApp* app, CwxMqTss* tss, CwxMsgBlock*& msg) {
  if (CwxEventInfo::CONN_CREATED == msg->event().getEvent()) { ///连接建立
    CwxAppChannel* channel = app->getInnerDispChannel();
    if (channel->isRegIoHandle(msg->event().getIoHandle())) {
      CWX_ERROR(("Handler[%d] is register, it's a big bug. exit....", msg->event().getIoHandle()));
      app->stop();
      return ;
    }
    CwxMqInnerDispHandler* pHandler = new CwxMqInnerDispHandler(app, channel,
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
      CWX_ERROR(("Failure to register sync handler[%d], from:%s:%u", pHandler->getHandle(),
          pHandler->m_strPeerHost.c_str(), pHandler->m_unPeerPort));
      delete pHandler;
      return ;
    }
    ///设置对象的tss对象
    pHandler->m_tss = (CwxMqTss*)CwxTss::instance();
    CWX_INFO(("Accept inner-sync connection from %s:%u", pHandler->m_strPeerHost.c_str(), pHandler->m_unPeerPort));
  } else if (EVENT_ZK_TOPIC_CHANGE == msg->event().getEvent()) { ///topic变化通知
    map<string, CWX_UINT8>* changedTopics = NULL;
    memcpy(&changedTopics, msg->rd_ptr(), sizeof(changedTopics));
    if (changedTopics->size()) {
      map<string, CWX_UINT8>::iterator iter = changedTopics->begin();
      for (;iter != changedTopics->end(); iter++) {
        map<CWX_UINT64, CwxMqInnerDispSession*>::iterator ses_iter = m_sessions.begin();
        for (;ses_iter != m_sessions.end();ses_iter++) {
          ses_iter->second->m_topicIsChanged = true;
          ///忽略topic/状态改变
          if ( iter->second == CWX_MQ_TOPIC_DELETE ||
              ses_iter->second->m_binlogCursor.find(iter->first) != ses_iter->second->m_binlogCursor.end()) continue;
          {///新增topic
            CwxBinLogMgr* binlogMgr = app->getTopicMgr()->getBinlogMgrByTopic(iter->first);
            if (!binlogMgr) {
              CWX_ERROR(("Failure to get binlogMgr by topic:%s", iter->first.c_str()));
              break;
            }
            CwxBinLogCursor* pCursor = binlogMgr->createCurser(ses_iter->second->m_ullStartSid);
            if (!pCursor) {
              CWX_ERROR(("Failure to createCursor by topic:%s", iter->first.c_str()));
              app->getTopicMgr()->freeBinlogMgrByTopic(iter->first, binlogMgr);
              break;
            }
            CwxBinlogMgrCursor* item = new CwxBinlogMgrCursor(iter->first, binlogMgr, pCursor);
            ses_iter->second->m_unReadyBinlogCursor.push_back(item);
            ses_iter->second->m_binlogCursor[iter->first] = item;
          }
        }
      }
    }
    delete changedTopics;
  } else {
    CWX_ERROR(("Recv unkown event:%d", msg->event().getEvent()));
    CWX_ASSERT(msg->event().getEvent() == CwxEventInfo::TIMEOUT_CHECK);
    CWX_ASSERT(msg->event().getSvrId() == CwxMqApp::SVR_TYPE_INNER_DISP);
  }
}

///释放关闭的session
void CwxMqInnerDispHandler::dealClosedSession(CwxMqApp* app, CwxMqTss* ) {
  list<CwxMqInnerDispSession*>::iterator iter;
  CwxMqInnerDispHandler* handler;
  ///获取用户object对象
  if (m_freeSession.begin() != m_freeSession.end()) {
    iter = m_freeSession.begin();
    while (iter != m_freeSession.end()) {
      CwxMqInnerDispSession* session = *iter;
      ///session必须是closed状态
      CWX_ASSERT((*iter)->m_bClosed);
      CWX_INFO(("Close sync session from host:%s", (*iter)->m_strHost.c_str()));
      if (session->m_binlogCursor.size()) {
        map<string, CwxBinlogMgrCursor*>::iterator cursor_iter = session->m_binlogCursor.begin();
        while(cursor_iter != session->m_binlogCursor.end()) {
          cursor_iter->second->m_binlogMgr->destoryCurser(cursor_iter->second->m_pCursor);
          app->getTopicMgr()->freeBinlogMgrByTopic(cursor_iter->first, cursor_iter->second->m_binlogMgr);
          delete cursor_iter->second;
          cursor_iter++;
        }
        session->m_binlogCursor.clear();
      }
      ///将session从session的map中删除
      m_sessions.erase((*iter)->m_ullSessionId);
      ///开始关闭连接
      map<CWX_UINT32, CwxMqInnerDispHandler*>::iterator conn_iter = (*iter)->m_conns.begin();
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
int CwxMqInnerDispHandler::onInput() {
  ///接受消息
  int ret = CwxMqInnerDispHandler::recvPackage(getHandle(), m_uiRecvHeadLen,
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
int CwxMqInnerDispHandler::onConnClosed() {
  ///一条连接关闭，则整个session失效
  if (m_syncSession) {
    ///如果连接对应的session存在
    if (m_sessions.find(m_ullSessionId) != m_sessions.end()) {
      ///CWX_INFO(("CwxMqDispHandler: conn closed, conn_id=%u", m_uiConnId));
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
int CwxMqInnerDispHandler::recvMessage() {
  if (CwxMqPoco::MSG_TYPE_INNER_SYNC_REPORT == m_header.getMsgType()) {
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

int CwxMqInnerDispHandler::recvReport(CwxMqTss* pTss) {
  int iRet = 0;
  CWX_UINT64 ullSid = 0;
  CWX_UINT32 uiChunk = 0;
  bool bZip = false;
  CwxMsgBlock* msg = NULL;
  do {
    if (!m_recvMsgData) {
      strcpy(pTss->m_szBuf2K, "No data.");
      CWX_ERROR(("Report package is empty, from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
    ///禁止重复report sid。若cunsor存在，表示已经报告过一次
    if (m_syncSession) {
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2048,
          "Can't report sync sid duplicate.");
      CWX_ERROR(("Report is duplicate, from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    ///若是同步sid的报告消息，则获取报告sid
    iRet = CwxMqPoco::parseInnerReportData(pTss->m_pReader,
        m_recvMsgData,
        ullSid,
        uiChunk,
        bZip,
        pTss->m_szBuf2K);
    if (CWX_MQ_ERR_SUCCESS != iRet) {
      CWX_ERROR(("Failure to parse report msg, err=%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    }
    CWX_INFO(("Report info :%s:%u, sid=%s, chunk=%u, zip=%s", m_strPeerHost.c_str(), m_unPeerPort, CwxCommon::toString(ullSid, pTss->m_szBuf2K, 10),
        uiChunk, bZip?"yes":"no"));
    m_syncSession = new CwxMqInnerDispSession(m_pApp);
    m_syncSession->m_strHost = m_strPeerHost;
    m_syncSession->m_uiChunk = uiChunk;
    m_syncSession->m_bZip = bZip;
    if (m_syncSession->m_uiChunk) {
      if (m_syncSession->m_uiChunk > CwxMqConfigCmn::MAX_CHUNK_SIZE_KB)
        m_syncSession->m_uiChunk = CwxMqConfigCmn::MAX_CHUNK_SIZE_KB;
      if (m_syncSession->m_uiChunk < CwxMqConfigCmn::MIN_CHUNK_SIZE_KB)
        m_syncSession->m_uiChunk = CwxMqConfigCmn::MIN_CHUNK_SIZE_KB;
      m_syncSession->m_uiChunk *= 1024;
    }
    m_syncSession->reformSessionId();
    ///将session加到session的map
    while (m_sessions.find(m_syncSession->m_ullSessionId) != m_sessions.end()) {
      m_syncSession->reformSessionId();
    }
    m_sessions[m_syncSession->m_ullSessionId] = m_syncSession;
    m_ullSessionId = m_syncSession->m_ullSessionId;
    m_syncSession->addConn(this);
    if (0 != m_syncSession->init()) {
      CWX_ERROR(("Failure to init session min heap."));
      break;
    }
    ///回复iRet的值
    iRet = CWX_MQ_ERR_SUCCESS;
    ///创建binlog的读取cursor
    list<string> topics;
    m_pApp->getTopicMgr()->getAllTopics(topics);
    for(list<string>::iterator iter = topics.begin(); iter != topics.end(); iter++) {
      CwxBinLogMgr* binlogMgr = m_pApp->getTopicMgr()->getBinlogMgrByTopic(*iter);
      if (!binlogMgr) { ///binlogMgr被删除
        CWX_ERROR(("Failure to get BinloMgr topic:%s", iter->c_str()));
        continue;
      }
      CwxBinLogCursor* pCursor = binlogMgr->createCurser(ullSid);
      if (!pCursor) {
        iRet = CWX_MQ_ERR_ERROR;
        strcpy(pTss->m_szBuf2K, "Failure to create cursor");
        CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
        m_pApp->getTopicMgr()->freeBinlogMgrByTopic(*iter, binlogMgr);
        break;
      }
      CwxBinlogMgrCursor* item  = new CwxBinlogMgrCursor(*iter, binlogMgr, pCursor);
      m_syncSession->m_binlogCursor[*iter] = item;
      m_syncSession->m_unReadyBinlogCursor.push_back(item);
    }
    m_syncSession->m_ullStartSid = ullSid;

    ///发送session id的消息
    if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packReportDataReply(pTss->m_pWriter,
        msg,
        m_header.getTaskId(),
        m_syncSession->m_ullSessionId,
        pTss->m_szBuf2K)) {
      CWX_ERROR(("Failure to pack sync data reply, err=%s, from:%s:%u", pTss->m_szBuf2K,
          m_strPeerHost.c_str(), m_unPeerPort));
      return -1;
    }
    msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    if (!putMsg(msg)) {
      CwxMsgBlockAlloc::free(msg);
      CWX_ERROR(("Failure push msg to send queue. from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
      return -1;
    }
    ///发送发一条binlog
    int iState = syncSendBinLog(pTss);
    if (-1 == iState) {
      iRet = CWX_MQ_ERR_ERROR;
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
    } else if (0 == iState) { ///产生continue的消息
      channel()->regRedoHander(this);
    }
    return 0;
  }while(0);
  ///到此一定错误
  CWX_ASSERT(CWX_MQ_ERR_SUCCESS != iRet);
  CwxMsgBlock* pBlock = NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncErr(pTss->m_pWriter,
      pBlock,
      m_header.getTaskId(),
      iRet,
      pTss->m_szBuf2K,
      pTss->m_szBuf2K)) {
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

int CwxMqInnerDispHandler::recvNewConnection(CwxMqTss* pTss) {
  int iRet = 0;
  CWX_UINT64 ullSession = 0;
  CwxMsgBlock* msg = NULL;
  do {
    if (!m_recvMsgData) {
      strcpy(pTss->m_szBuf2K, "No data.");
      CWX_ERROR(("Session connect-report package is empty, from:%s:%u", m_strPeerHost.c_str(), m_unPeerPort));
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
      CwxCommon::snprintf(pTss->m_szBuf2K, 2048, "Session[%s] doesn't exist.",
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
    } else if (0 == iState) { ///产生continue消息
      channel()->regRedoHander(this);
    }
    return 0;
  } while(0);
  ///到此一定错误
  CWX_ASSERT(CWX_MQ_ERR_SUCCESS != iRet);
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncErr(pTss->m_pWriter,
      msg,
      m_header.getTaskId(),
      iRet,
      pTss->m_szBuf2K,
      pTss->m_szBuf2K)) {
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

int CwxMqInnerDispHandler::recvReply(CwxMqTss* pTss) {
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
    if (ullSeq != m_ullSendSeq) {
      char szTmp1[64];
      char szTmp2[64];
      iRet = CWX_MQ_ERR_ERROR;
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
        "Seq[%s] is not same with the connection's[%s].",
        CwxCommon::toString(ullSeq, szTmp1, 10),
        CwxCommon::toString(m_ullSendSeq, szTmp2, 10));
      CWX_ERROR(("%s, from:%s:%u", pTss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
      break;
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
int CwxMqInnerDispHandler::onRedo() {
  ///发送下一条binlog
  int iState = syncSendBinLog(m_tss);
  if (-1 == iState) {
    CWX_ERROR(("%s, from:%s:%u", m_tss->m_szBuf2K, m_strPeerHost.c_str(), m_unPeerPort));
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
  return 0;
}

///0：未发送一条binlog；
///1：发送了一条binlog；
///-1：失败；
int CwxMqInnerDispHandler::syncSendBinLog(CwxMqTss* pTss) {
  int iRet = 0;
  CwxMsgBlock* pBlock = NULL;
  CWX_UINT32 uiKeyLen = 0;
  CWX_UINT32 uiTotalLen = 0;
  CWX_UINT64 ullSeq = m_syncSession->m_ullSeq;
  if (m_syncSession->m_topicIsChanged) {
    syncTopicState(pTss);
  }

  if (m_syncSession->m_uiChunk) pTss->m_pWriter->beginPack();
  while (1) {
    if (m_syncSession->m_unReadyBinlogCursor.size()) {
      if (syncSeekToReportSid(pTss) != 0) return -1;
    }
    if (1 != (iRet = syncSeekToBinlog(pTss))) break;
    //设置移到下一个记录位置
    m_syncSession->m_bNext = true;
    if (!m_syncSession->m_uiChunk) {
      iRet = syncPackOneBinLog(pTss->m_pWriter,
        pBlock,
        ullSeq,
        pTss->m_pBinlogData,
        pTss->m_szBuf2K);
      m_ullLastSid = m_syncSession->m_pBinlogCursor->m_pCursor->getHeader().getSid();
      break;
    } else {
      iRet = syncPackMultiBinLog(pTss->m_pWriter,
        pTss->m_pItemWriter,
        pTss->m_pBinlogData,
        uiKeyLen,
        pTss->m_szBuf2K);
      m_ullLastSid = m_syncSession->m_pBinlogCursor->m_pCursor->getHeader().getSid();
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
  pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_INNER_DISP);
  pBlock->send_ctrl().setHostId(0);
  pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (!putMsg(pBlock)) {
    CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Failure to send binlog");
    CWX_ERROR((pTss->m_szBuf2K));
    CwxMsgBlockAlloc::free(pBlock);
    return -1;
  }
  m_ullSendSeq = ullSeq;
  m_syncSession->m_ullSeq++;
  return 1; ///发送了一条消息
}

//0:成功；-1：错误
int CwxMqInnerDispHandler::syncSeekToReportSid(CwxMqTss* tss) {
  if (m_syncSession->m_unReadyBinlogCursor.size() < 1) return 0;
  int iRet = 0;
  CwxBinlogMgrCursor* out = NULL;
  list<CwxBinlogMgrCursor*>::iterator iter = m_syncSession->m_unReadyBinlogCursor.begin();
  while(iter != m_syncSession->m_unReadyBinlogCursor.end()) {
    CwxBinlogMgrCursor* item = *iter;
    if (item->m_pCursor->isUnseek()) { ///若binlog的读取cursor悬空，则定位
      if (m_syncSession->m_ullStartSid < item->m_binlogMgr->getMaxSid()) {
        iRet = item->m_binlogMgr->seek(item->m_pCursor, m_syncSession->m_ullStartSid);
        if (-1 == iRet) {
          CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Failure to seek, err:%s",
              item->m_pCursor->getErrMsg());
          CWX_ERROR((tss->m_szBuf2K));
          return -1;
        }else if (0 == iRet) {
          char szBuf1[64];
          char szBuf2[64];
          CwxCommon::snprintf(tss->m_szBuf2K, 2047,
              "Should seek topic[%s] to sid[%s] with max_sid[%s], but not.",
              CwxCommon::toString(m_syncSession->m_ullStartSid, szBuf1),
              CwxCommon::toString(item->m_binlogMgr->getMaxSid(), szBuf2));
          CWX_ERROR((tss->m_szBuf2K));
        } else {
          ///若定位成功
          bool success = false;
          do {
            if (m_syncSession->m_ullStartSid == item->m_pCursor->getHeader().getSid()) {
              iRet = item->m_binlogMgr->next(item->m_pCursor);
              if (1!= iRet) break;
            }
            ///添加到最小堆
            if (m_syncSession->m_heap.push(item, out)) {
              CWX_ERROR(("Heap is full, topic:%s is pop out.", out->m_strTopic.c_str())); ///有元素移除
            }
            m_syncSession->m_unReadyBinlogCursor.erase(iter++);
            success = true;
          }while(0);
          if(success) continue;
        }
      }else { ///已经删除的topic并且不在发送范围则删除该binlog应用
        if (item->m_binlogMgr->getZkTopicState() == CWX_MQ_TOPIC_DELETE) {
          item->m_binlogMgr->destoryCurser(item->m_pCursor);
          m_pApp->getTopicMgr()->freeBinlogMgrByTopic(item->m_strTopic, item->m_binlogMgr);
          m_syncSession->m_binlogCursor.erase(item->m_strTopic);
          m_syncSession->m_unReadyBinlogCursor.erase(iter++);
          assert(m_syncSession->m_binlogCursor.size() == (m_syncSession->m_unReadyBinlogCursor.size()
              + m_syncSession->m_heap.count()));
          m_syncSession->m_topicIsChanged = true; ///需要发送给slave目前topic/state信息
          continue;
        }
      }
    } else { ///cursor未悬空，没有可读数据
      iRet = item->m_binlogMgr->next(item->m_pCursor);
      if (1 == iRet) { ///定位成功
        if (m_syncSession->m_heap.push(item, out)) {
          CWX_ERROR(("Heap is full, topic:%s is pop out.", out->m_strTopic.c_str()));
        }
        m_syncSession->m_unReadyBinlogCursor.erase(iter++);
        continue;
      }
      if (0 == iRet && item->m_binlogMgr->getZkTopicState() == CWX_MQ_TOPIC_DELETE) { ///定位到最后，且topic已经被删除则取消
        item->m_binlogMgr->destoryCurser(item->m_pCursor);
        m_pApp->getTopicMgr()->freeBinlogMgrByTopic(item->m_strTopic, item->m_binlogMgr);
        m_syncSession->m_binlogCursor.erase(item->m_strTopic);
        m_syncSession->m_unReadyBinlogCursor.erase(iter++);
        assert(m_syncSession->m_binlogCursor.size() == (m_syncSession->m_unReadyBinlogCursor.size()
            + m_syncSession->m_heap.count()));
        m_syncSession->m_topicIsChanged = true; ///需要发送给slave目前topic/state信息
        continue;
      }
    }
    iter++; ///定位失败，处理下一个
  }
  return 0;
}

///-1：失败，1：成功
int CwxMqInnerDispHandler::syncPackOneBinLog(CwxPackageWriter* writer,
                                        CwxMsgBlock*& block,
                                        CWX_UINT64 ullSeq,
                                        CwxKeyValueItem const* pData,
                                        char* szErr2K)
{
  ///形成binlog发送的数据包
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncData(writer,
    block,
    0,
    m_syncSession->m_pBinlogCursor->m_pCursor->getHeader().getSid(),
    m_syncSession->m_pBinlogCursor->m_pCursor->getHeader().getDatetime(),
    m_syncSession->m_pBinlogCursor->m_strTopic.c_str(),
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
int CwxMqInnerDispHandler::syncPackMultiBinLog(CwxPackageWriter* writer,
                                          CwxPackageWriter* writer_item,
                                          CwxKeyValueItem const* pData,
                                          CWX_UINT32& uiLen,
                                          char* szErr2K)
{
  ///形成binlog发送的数据包
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataItem(writer_item,
    m_syncSession->m_pBinlogCursor->m_pCursor->getHeader().getSid(),
    m_syncSession->m_pBinlogCursor->m_strTopic.c_str(),
    m_syncSession->m_pBinlogCursor->m_pCursor->getHeader().getDatetime(),
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

///1:发现记录；0：没有发现；-1：错误
int CwxMqInnerDispHandler::syncSeekToBinlog(CwxMqTss* tss) {
  int iRet = 0;
  CwxBinlogMgrCursor* item;
  if (!m_syncSession->m_heap.pop(item))  return 0;
  m_syncSession->m_unReadyBinlogCursor.push_back(item);
  m_syncSession->m_pBinlogCursor = item;
  CWX_UINT32 uiDataLen = item->m_pCursor->getHeader().getLogLen();
  ///准备data读取的buf
  char* szData = tss->getBuf(uiDataLen);
  ///读取data
  iRet = item->m_binlogMgr->fetch(item->m_pCursor, szData, uiDataLen);
  if (-1 == iRet) { ///读取失败
    CwxCommon::snprintf(tss->m_szBuf2K, 2047, "Failure to fetch data, topic:%s, err:%s",
        item->m_strTopic.c_str(), item->m_pCursor->getErrMsg());
    CWX_ERROR(("tss->m_szBuf2K"));
    return -1;
  }
  if (!tss->m_pReader->unpack(szData, uiDataLen, false, true)) {
    CWX_ERROR(("Can't unpack binlog, sid=%s", CwxCommon::toString(item->m_pCursor->getHeader().getSid(),
        tss->m_szBuf2K)));
    return 0;
  }
  ///获取CWX_MQ_D的key,此为真正的data
  tss->m_pBinlogData = tss->m_pReader->getKey(CWX_MQ_D);
  if (!tss->m_pBinlogData) {
    CWX_ERROR(("Can't find key[%s] in binlog, sid=%s", CWX_MQ_D,
        CwxCommon::toString(item->m_pCursor->getHeader().getSid(), tss->m_szBuf2K)));
    return 0;
  }
  return 1;
}

///发送topic状态表
int CwxMqInnerDispHandler::syncTopicState(CwxMqTss* pTss) {
  if (m_syncSession->m_topicIsChanged) {
    map<string, CWX_UINT8> topicState;
    map<string, CwxBinlogMgrCursor*>::iterator iter = m_syncSession->m_binlogCursor.begin();
    while (iter != m_syncSession->m_binlogCursor.end()) {
      topicState.insert(pair<string, CWX_UINT8>(iter->first, iter->second->m_binlogMgr->getZkTopicState()));
      CWX_DEBUG(("sync topic:%s, state:%d", iter->first.c_str(), iter->second->m_binlogMgr->getZkTopicState()));
      iter++;
    }
    CwxMsgBlock* pBlock;
    int ret = CwxMqPoco::packTopicState(pTss->m_pWriter, pTss->m_pItemWriter, pBlock, 0, topicState, pTss->m_szBuf2K);
    if (ret != CWX_MQ_ERR_SUCCESS) {
      CWX_ERROR(("Failure to pack topicState err:%s", pTss->m_szBuf2K));
      return -1;
    }
    ///根据svr类型，发送数据包
    pBlock->send_ctrl().setConnId(CWX_APP_INVALID_CONN_ID);
    pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_INNER_DISP);
    pBlock->send_ctrl().setHostId(0);
    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    if (!putMsg(pBlock)) {
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Failure to send topicState msg.");
      CWX_ERROR((pTss->m_szBuf2K));
      CwxMsgBlockAlloc::free(pBlock);
      return -1;
    }
    m_syncSession->m_topicIsChanged = false;
  }
  return 0;
}
