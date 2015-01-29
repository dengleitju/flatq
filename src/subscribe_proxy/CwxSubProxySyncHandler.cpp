#include "CwxSubProxySyncHandler.h"
#include "CwxSubProxyApp.h"
#include "CwxZlib.h"
#include "CwxMqConnector.h"

/**
@brief 连接可读事件，返回-1，close()会被调用
@return -1：处理失败，会调用close()； 0：处理成功
*/
int CwxSubSyncHandler::onInput() {
  ///接受消息
  int ret = CwxAppHandler4Channel::recvPackage(getHandle(),
    m_uiRecvHeadLen,
    m_uiRecvDataLen,
    m_szHeadBuf,
    m_header,
    m_recvMsgData);
  if (1 != ret) return ret; ///如果失败或者消息没有接收完，返回。
  ///通知收到一个消息
  ret = recvMessage();
  ///如果m_recvMsgData没有释放，则是否m_recvMsgData等待接收下一个消息
  if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
  this->m_recvMsgData = NULL;
  this->m_uiRecvHeadLen = 0;
  this->m_uiRecvDataLen = 0;
  return ret;
}
/**
@brief 通知连接关闭。
@return 1：不从engine中移除注册；0：从engine中移除注册但不删除handler；-1：从engine中将handle移除并删除。
*/
int CwxSubSyncHandler::onConnClosed() {
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)m_pTss->m_userData;
  ///将session的重连标志设置为true，以便进行重新连接
  pSession->m_bNeedClosed = true;
  ///将连接从连接map中删除
  pSession->m_conns.erase(m_uiConnId);
  return -1;
}

//关闭已有连接
void CwxSubSyncHandler::closeSession(CwxMqTss* pTss){
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)pTss->m_userData;
  ///CWX_INFO(("Close session source:%s", pSession->m_strSource.c_str()));
  ///将session的重连标志设置为true，以便进行重新连接
  pSession->m_bClosed = true;
  {// 关闭连接
    map<CWX_UINT32, CwxSubSyncHandler*>::iterator iter = pSession->m_conns.begin();
    while(iter != pSession->m_conns.end()){
      iter->second->close(); // CwxSubSyncHandler::onConnClosed会将连接删除
      iter = pSession->m_conns.begin();
    }
    pSession->m_conns.clear();
  }
  {// 释放消息
    map<CWX_UINT64/*seq*/, CwxMsgBlock*>::iterator iter = pSession->m_msg.begin();
    while (iter != pSession->m_msg.end()) {
      CwxMsgBlockAlloc::free(iter->second);
      iter++;
    }
    pSession->m_msg.clear();
  }
  // 清理环境
  pSession->m_bNeedClosed = false;
  pSession->m_ullSessionId = 0;
  pSession->m_ullNextSeq = 0;
}

///创建与mq同步的连接。返回值：0：成功；-1：失败
int CwxSubSyncHandler::createSession(CwxMqTss* pTss){
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)pTss->m_userData;
  pSession->m_uiLastConnectTimestamp = time(NULL);
  CWX_ASSERT((pSession->m_bClosed));
  ///重建所有连接
  CwxINetAddr addr;
  if (0 != addr.set(pSession->m_syncHost.getPort(), pSession->m_syncHost.getHostName().c_str())){
    CWX_ERROR(("Failure to init addr, addr:%s, port:%u, err=%d",
      pSession->m_syncHost.getHostName().c_str(),
      pSession->m_syncHost.getPort(),
      errno));
    return -1;
  }
  CWX_INFO(("Create session to mq[%s:%d], source:%s", pSession->m_syncHost.getHostName().c_str(),
      pSession->m_syncHost.getPort(), pSession->m_strSource.c_str()));
  CWX_UINT32 i = 0;
  CWX_UINT32 unConnNum = pSession->m_pApp->getConfig().getCommon().m_uiSyncNum;
  if (unConnNum < 1) unConnNum = 1;
  int* fds = new int[unConnNum];
  for (i = 0; i < unConnNum; i++) {
    fds[i] = -1;
  }
  CwxTimeValue timeout(CWX_MQ_CONN_TIMEOUT_SECOND);
  CwxTimeouter timeouter(&timeout);
  if (0 != CwxMqConnector::connect(addr, unConnNum, fds, &timeouter, true)) {
    CWX_ERROR(("Failure to connect to addr:%s, port:%u, err=%d",
      pSession->m_syncHost.getHostName().c_str(),
      pSession->m_syncHost.getPort(),
      errno));
    return -1;
  }
  ///注册连接id
  int ret = 0;
  CWX_UINT32 uiConnId = 0;
  CwxAppHandler4Channel* handler = NULL;
  for (i = 0; i < unConnNum; i++) {
    uiConnId = i + 1;
    handler = new CwxSubSyncHandler(pTss, pSession->m_channel, uiConnId);
    handler->setHandle(fds[i]);
    if (0 != handler->open()) {
      CWX_ERROR(("Failure to register handler[%d]", handler->getHandle()));
      delete handler;
      break;
    }
    pSession->m_conns[uiConnId] = (CwxSubSyncHandler*)handler;
  }
  if (i != unConnNum) { ///失败
    for (; i < unConnNum; i++)  ::close(fds[i]);
    closeSession(pTss);
    return -1;
  }
  if (pSession->m_bNewly && pSession->m_ullLogSid) pSession->m_bNewly = false;
  ///发送report的消息
  ///创建往master报告sid的通信数据包
  CwxMsgBlock* pBlock = NULL;
  ret = CwxMqPoco::packOuterReportData(pTss->m_pWriter,
    pBlock,
    0,
    pSession->m_bNewly,
    pSession->m_ullLogSid,
    pSession->m_strTopic.c_str(),
    pSession->m_uiChunkSize,
    pSession->m_strSource.c_str(),
    pSession->m_bZip,
    pTss->m_szBuf2K);
  //清空session 关闭相关的标记
  pSession->m_bClosed = false;
  pSession->m_bNeedClosed = false;

  if (ret != CWX_MQ_ERR_SUCCESS) {    ///数据包创建失败
    CWX_ERROR(("Failure to create report package, err:%s", pTss->m_szBuf2K));
    closeSession(pTss);
    return -1;
  } else {
    handler = pSession->m_conns.begin()->second;
    pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
    if (!handler->putMsg(pBlock)){
      CWX_ERROR(("Failure to report sid to mq"));
      CwxMsgBlockAlloc::free(pBlock);
      closeSession(pTss);
      return -1;
    }
  }
  return 0;
}


///接收消息，0：成功；-1：失败
int CwxSubSyncHandler::recvMessage(){
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)m_pTss->m_userData;
  ///如果处于关闭状态，直接返回-1关闭连接
  if (pSession->m_bNeedClosed) return -1;
  if (!m_recvMsgData || !m_recvMsgData->length()) {
    CWX_ERROR(("Receive empty msg from master"));
    return -1;
  }
  m_recvMsgData->event().setConnId(m_uiConnId);
  //SID报告的回复，此时，一定是报告失败
  if (CwxMqPoco::MSG_TYPE_SYNC_DATA == m_header.getMsgType() ||
    CwxMqPoco::MSG_TYPE_SYNC_DATA_CHUNK == m_header.getMsgType())
  {
    list<CwxMsgBlock*> msgs;
    if (0 != recvMsg(m_recvMsgData, msgs)) return -1;
    m_recvMsgData = NULL;
    CwxMsgBlock* block = NULL;
    int ret = 0;
    list<CwxMsgBlock*>::iterator msg_iter = msgs.begin();
    while (msg_iter != msgs.end()) {
      block = *msg_iter;
      if (CwxMqPoco::MSG_TYPE_SYNC_DATA  == m_header.getMsgType()) {
        ret = dealSyncData(block);
      } else {
        ret = dealSyncChunkData(block);
      }
      msgs.pop_front();
      msg_iter = msgs.begin();
      if (block) CwxMsgBlockAlloc::free(block);
      if (0 != ret) break;
    }
    if (0 != ret){
      if (msg_iter != msgs.end()) {
        while (msg_iter != msgs.end()) {
          block = *msg_iter;
          CwxMsgBlockAlloc::free(block);
          msg_iter++;
        }
      }
      return -1;
    }
    return 0;
  } else if (CwxMqPoco::MSG_TYPE_SYNC_REPORT_REPLY  == m_header.getMsgType()) {
    return (0 != dealSyncReportReply(m_recvMsgData))?-1:0;
  } else if (CwxMqPoco::MSG_TYPE_SYNC_ERR  == m_header.getMsgType()) {
    dealErrMsg(m_recvMsgData);
    return -1;
  }
  CWX_ERROR(("Receive invalid msg type from master, msg_type=%u", m_header.getMsgType()));
  return -1;
}

int CwxSubSyncHandler::recvMsg(CwxMsgBlock*& msg, list<CwxMsgBlock*>& msgs) {
  CWX_UINT64 ullSeq = 0;
  if (msg->length() < sizeof(ullSeq)) {
    CWX_ERROR(("sync data's size[%u] is invalid, min-size=%u", msg->length(), sizeof(ullSeq)));
    return -1;
  }
  ullSeq = CwxMqPoco::getSeq(msg->rd_ptr());
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)m_pTss->m_userData;
  if (!pSession->recv(ullSeq, msg, msgs)) {
    CWX_ERROR(("Conn-id[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  return 0;
}

int CwxSubSyncHandler::dealErrMsg(CwxMsgBlock*& msg) {
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)m_pTss->m_userData;
  if (pSession->m_conns.find(msg->event().getConnId()) == pSession->m_conns.end()) {
    CWX_ERROR(("Conn-id[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  int ret = 0;
  char const* szErrMsg;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseSyncErr(m_pTss->m_pReader,
    msg,
    ret,
    szErrMsg,
    m_pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to parse err msg from mq, err=%s", m_pTss->m_szBuf2K));
    return -1;
  }
  CWX_ERROR(("Failure to sync from mq, ret=%d, err=%s", ret, szErrMsg));
  ///设置消息头发送给client
  CwxMsgHead header(m_header);
  header.setMsgType(CwxMqPoco::MSG_TYPE_SYNC_ERR);
  header.setTaskId(msg->event().getConnId());
  header.setDataLen(msg->length());
  CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(msg->length() + CwxMsgHead::MSG_HEAD_LEN);
  memcpy(pBlock->wr_ptr(), header.toNet(), CwxMsgHead::MSG_HEAD_LEN);
  pBlock->wr_ptr(CwxMsgHead::MSG_HEAD_LEN);
  memcpy(pBlock->wr_ptr(), msg->rd_ptr(), msg->length());
  pBlock->wr_ptr(msg->length());
  pBlock->send_ctrl().setConnId(pSession->m_uiReplyConnId);
  pBlock->send_ctrl().setSvrId(CwxSubProxyApp::SVR_SUB_TYPE_RECV);
  pBlock->send_ctrl().setHostId(msg->event().getHostId());
  if (-1 == pSession->m_pApp->sendMsgByConn(pBlock)) {
    CWX_ERROR(("Failure to send msg to conn:%d", pSession->m_uiReplyConnId));
    return -1;
  }
  return 0;
}

///处理Sync report的reply消息。返回值：0：成功；-1：失败
int CwxSubSyncHandler::dealSyncReportReply(CwxMsgBlock*& msg)
{
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)m_pTss->m_userData;
  if (pSession->m_conns.find(msg->event().getConnId()) == pSession->m_conns.end()) {
    CWX_ERROR(("Conn-id[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  CWX_UINT64 ullSessionId;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseReportDataReply(m_pTss->m_pReader,
    msg,
    ullSessionId,
    m_pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to parse report-reply msg from master, err=%s", m_pTss->m_szBuf2K));
    return -1;
  }
  pSession->m_ullSessionId = ullSessionId;
  ///其他连接报告
  map<CWX_UINT32, CwxSubSyncHandler*>::iterator iter = pSession->m_conns.begin();
  while (iter != pSession->m_conns.end()) {
    if (iter->first != m_uiConnId) {
      CwxMsgBlock* pBlock = NULL;
      int ret = CwxMqPoco::packReportNewConn(m_pTss->m_pWriter,
        pBlock,
        0,
        ullSessionId,
        m_pTss->m_szBuf2K);
      if (ret != CWX_MQ_ERR_SUCCESS) { ///数据包创建失败
        CWX_ERROR(("Failure to create report package, err:%s",
          m_pTss->m_szBuf2K));
        return -1;
      } else {
        pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
        if (!iter->second->putMsg(pBlock)){
          CWX_ERROR(("Failure to report new session connection to mq"));
          CwxMsgBlockAlloc::free(pBlock);
          return -1;
        }
      }
    }
    iter++;
  }
  return 0;
}

///处理收到的sync data。返回值：0：成功；-1：失败
int CwxSubSyncHandler::dealSyncData(CwxMsgBlock*& msg)
{
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)m_pTss->m_userData;
  if (pSession->m_conns.find(msg->event().getConnId()) == pSession->m_conns.end()){
    CWX_ERROR(("Connection[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  ///设置消息包头发送给client
  CwxMsgHead header(m_header);
  header.setMsgType(CwxMqPoco::MSG_TYPE_PROXY_SYNC_DATA);
  header.setTaskId(msg->event().getConnId());
  header.setDataLen(msg->length());
  CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(msg->length() + CwxMsgHead::MSG_HEAD_LEN);
  memcpy(pBlock->wr_ptr(), header.toNet(), CwxMsgHead::MSG_HEAD_LEN);
  pBlock->wr_ptr(CwxMsgHead::MSG_HEAD_LEN);
  memcpy(pBlock->wr_ptr(), msg->rd_ptr(), msg->length());
  pBlock->wr_ptr(msg->length());
  pBlock->send_ctrl().setConnId(pSession->m_uiReplyConnId);
  pBlock->send_ctrl().setSvrId(CwxSubProxyApp::SVR_SUB_TYPE_RECV);
  pBlock->send_ctrl().setHostId(msg->event().getHostId());
  if (-1 == pSession->m_pApp->sendMsgByConn(pBlock)) {
    CWX_ERROR(("Failure to send msg to conn:%d", pSession->m_uiReplyConnId));
    return -1;
  }
  return 0;
}

//处理收到的chunk模式下的sync data。返回值：0：成功；-1：失败
int CwxSubSyncHandler::dealSyncChunkData(CwxMsgBlock*& msg)
{
  CwxSubSyncSession* pSession = (CwxSubSyncSession*)m_pTss->m_userData;
  if (pSession->m_conns.find(msg->event().getConnId()) == pSession->m_conns.end()){
    CWX_ERROR(("Connection[%u] doesn't exist.", msg->event().getConnId()));
    return -1;
  }
  ///设置消息包头发送给client
  CwxMsgHead header(m_header);
  header.setMsgType(CwxMqPoco::MSG_TYPE_PROXY_SYNC_CHUNK_DATA);
  header.setTaskId(msg->event().getConnId());
  header.setDataLen(msg->length());
  CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(msg->length() + CwxMsgHead::MSG_HEAD_LEN);
  memcpy(pBlock->wr_ptr(), header.toNet(), CwxMsgHead::MSG_HEAD_LEN);
  pBlock->wr_ptr(CwxMsgHead::MSG_HEAD_LEN);
  memcpy(pBlock->wr_ptr(), msg->rd_ptr(), msg->length());
  pBlock->wr_ptr(msg->length());
  pBlock->send_ctrl().setConnId(pSession->m_uiReplyConnId);
  pBlock->send_ctrl().setSvrId(CwxSubProxyApp::SVR_SUB_TYPE_RECV);
  pBlock->send_ctrl().setHostId(msg->event().getHostId());
  if (-1 == pSession->m_pApp->sendMsgByConn(pBlock)) {
    CWX_ERROR(("Failure to send msg to conn:%d", pSession->m_uiReplyConnId));
    return -1;
  }
  return 0;
}

int CwxSubSyncHandler::dealSyncDataReply(CwxMsgBlock*& msg, CwxSubSyncSession* pSession) {
  map<CWX_UINT32, CwxSubSyncHandler*>::iterator iter = pSession->m_conns.find(msg->event().getMsgHeader().getTaskId());
  if (iter == pSession->m_conns.end()){
    CWX_ERROR(("Connection[%u] doesn't exist.", msg->event().getMsgHeader().getTaskId()));
    return -1;
  }
  if (msg->length() != sizeof(CWX_UINT64) * 2) {
    CWX_ERROR(("Client sync data reply msg length[%u] < ullSeq + ullSid", msg->length()));
    return -1;
  }
  CWX_UINT64 ullSeq = CwxMqPoco::getSeq(msg->rd_ptr());
  CWX_UINT64 ullSid = CwxMqPoco::getSeq(msg->rd_ptr() + sizeof(ullSeq));
  if (ullSid > pSession->m_ullLogSid) pSession->m_ullLogSid = ullSid;
  //回复mq发送者
  CwxMsgBlock* reply_block = NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataReply(iter->second->m_pTss->m_pWriter,
    reply_block,
    0,
    CwxMqPoco::MSG_TYPE_SYNC_DATA_REPLY,
    ullSeq,
    iter->second->m_pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to pack sync data reply, errno=%s", iter->second->m_pTss->m_szBuf2K));
    return -1;
  }
  reply_block->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (!iter->second->putMsg(reply_block))
  {
    CWX_ERROR(("Failure to send sync data reply to mq"));
    CwxMsgBlockAlloc::free(reply_block);
    return -1;
  }
  return 0;
}

int CwxSubSyncHandler::dealSyncChunkDataRely(CwxMsgBlock*& msg, CwxSubSyncSession* pSession) {
  map<CWX_UINT32, CwxSubSyncHandler*>::iterator iter = pSession->m_conns.find(msg->event().getMsgHeader().getTaskId());
  if (iter == pSession->m_conns.end()){
    CWX_ERROR(("Connection[%u] doesn't exist.", msg->event().getMsgHeader().getTaskId()));
    return -1;
  }
  if (msg->length() != sizeof(CWX_UINT64) * 2) {
    CWX_ERROR(("Client sync data reply msg length[%u] < ullSeq + ullSid", msg->length()));
    return -1;
  }
  CWX_UINT64 ullSeq = CwxMqPoco::getSeq(msg->rd_ptr());
  CWX_UINT64 ullSid = CwxMqPoco::getSeq(msg->rd_ptr() + sizeof(ullSeq));
  if (ullSid > pSession->m_ullLogSid) pSession->m_ullLogSid = ullSid;
  //回复mq发送者
  CwxMsgBlock* reply_block = NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncDataReply(iter->second->m_pTss->m_pWriter,
    reply_block,
    0,
    CwxMqPoco::MSG_TYPE_SYNC_DATA_CHUNK_REPLY,
    ullSeq,
    iter->second->m_pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to pack sync data reply, errno=%s", iter->second->m_pTss->m_szBuf2K));
    return -1;
  }
  reply_block->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (!iter->second->putMsg(reply_block))
  {
    CWX_ERROR(("Failure to send sync data reply to mq"));
    CwxMsgBlockAlloc::free(reply_block);
    return -1;
  }
  return 0;
}
