#include "CwxProductProxyRecvHandler.h"
#include "CwxProductProxyApp.h"
#include "CwxMqPoco.h"
#include "CwxZlib.h"

int CwxProProxyRecvHandler::onRecvMsg(CwxMsgBlock*& msg,  CwxTss* pThrEnv) {
  CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
  int iRet = CWX_MQ_ERR_ERROR;
  CWX_UINT32 uiTaskId = 0;
  do{
    ///binlog接收消息
    if (CwxMqPoco::MSG_TYPE_PROXY_RECV_DATA == msg->event().getMsgHeader().getMsgType()) {
      if (m_pApp->getMqThreadPool()->getQueuedMsgNum() >= m_pApp->getConfig().getCmmon().m_uiMaxQueueNum){ ///发送队列满
        CwxMsgBlock* replyMsg = NULL;
        CWX_INFO(("Thread send queue is full."));
        if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packProxyRecvDataReply(pTss->m_pWriter,
            replyMsg,
            0,
            CWX_MQ_ERR_ERROR,
            0,
            "Send queue full",
            pTss->m_szBuf2K
            ))
        {
          CWX_ERROR(("Failure to pack mq reply, err:%s", pTss->m_szBuf2K));
          m_pApp->noticeCloseConn(msg->event().getConnId());
        }else {
          CwxProProxyRecvHandler::reply(m_pApp, replyMsg, msg->event().getConnId());
        }
        return 0;
      }
      CwxKeyValueItem const* pData;
      char const* szTopic;
      char const* szGroup;
      if (!msg) {
        strcpy(pTss->m_szBuf2K, "No data.");
        CWX_ERROR((pTss->m_szBuf2K));
        break;
      }
      unsigned long ulUnzipLen = 0;
      bool bZip = msg->event().getMsgHeader().isAttr(CwxMsgHead::ATTR_COMPRESS);
      ///判断是否压缩数据
      if (bZip) { ///压缩数据，需要解压
        ///首先准备解压Buf
        if (!prepareUnzipBuf()) {
          CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
              "Failure to prepare unzip buf, size:%u", m_uiBufLen);
          CWX_ERROR((pTss->m_szBuf2K));
          break;
        }
        ulUnzipLen = m_uiBufLen;
        ///解压
        if (!CwxZlib::unzip(m_unzipBuf, ulUnzipLen,
            (const unsigned char*)msg->rd_ptr(), msg->length())) {
          CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
              "Failure to unzip recv msg, msg size:%u, buf size:%u",
              msg->length(), m_uiBufLen);
          CWX_ERROR((pTss->m_szBuf2K));
          break;
        }
      }
      if (CWX_MQ_ERR_SUCCESS != (iRet = CwxMqPoco::parseProxyRecvData(pTss->m_pReader,
          bZip ? (char const*)m_unzipBuf : msg->rd_ptr(),
          bZip ? ulUnzipLen : msg->length(),
          pData,
          szGroup,
          szTopic,
          pTss->m_szBuf2K))) {
        CWX_ERROR(("Failure to parse the recieve msg, err=%s", pTss->m_szBuf2K));
        break;
      }
      CWX_INT32 conn_id = 0;
      if (szGroup == NULL) { ///轮训发送
        map<string, CwxMqTopicGroup>::iterator iter = m_mqTopic->find(szTopic);
        if (iter == m_mqTopic->end()) {
          CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Failure to get topic:%s group-mq.", szTopic);
          CWX_ERROR((pTss->m_szBuf2K));
          break;
        }
        szGroup = iter->second.getNextGroup().c_str();
        map<string, CwxMqGroupConn>::iterator conn_iter;
        string group = szGroup;
        do {
          conn_iter = m_mqGroup.find(group);
         if (conn_iter == m_mqGroup.end() || conn_iter->second.m_uiMqConnId <= 0) {
           group = iter->second.getNextGroup();
         }else {
           conn_id = conn_iter->second.m_uiMqConnId;
           szGroup = conn_iter->first.c_str();
           break;
         }
        }while(strcmp(szGroup, group.c_str()) != 0);
      }else {
        map<string, CwxMqGroupConn>::iterator iter = m_mqGroup.find(szGroup);
        if (iter == m_mqGroup.end() || iter->second.m_uiMqConnId <= 0) {
          iRet = CWX_MQ_ERR_ERROR;
          CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
              "Group:%s is not connected.", szGroup);
          CWX_ERROR((pTss->m_szBuf2K));
          break;
        }
        conn_id = iter->second.m_uiMqConnId;
      }

      if (conn_id <= 0) {
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Failure to topic :%s group-mq.", szTopic);
        CWX_ERROR((pTss->m_szBuf2K));
        break;
      }
      CwxMsgBlock* sndMsg = NULL;
      uiTaskId = m_pApp->getNextTaskId();
      if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packRecvData(pTss->m_pWriter,
          sndMsg,
          uiTaskId,
          szTopic,
          *pData,
          true,
          pTss->m_szBuf2K)) {
        CWX_ERROR((pTss->m_szBuf2K));
        break;
      }
      CwxProProxyTask* pTask = new CwxProProxyTask(m_pApp, &m_pApp->getTaskBoard());
      pTask->m_sndMsg = sndMsg;
      pTask->setTaskId(uiTaskId);
      pTask->m_uiMsgTaskId = msg->event().getMsgHeader().getTaskId();
      pTask->m_uiReplyConnId = msg->event().getConnId();
      pTask->m_strMqGroup = szGroup;
      pTask->m_strMqTopic = szTopic;
      pTask->m_uiMqConnId = conn_id;
      pTask->execute(pTss);
      return 1;
   }else {
     CWX_ERROR(("Recv unkown msg type:%d", msg->event().getMsgHeader().getMsgType()));
   }
  }while(0);
  ///发生错误，打包返回给client
  CwxMsgBlock* pBlock=NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packProxyRecvDataReply(pTss->m_pWriter,
      pBlock,
      msg->event().getMsgHeader().getTaskId(),
      CWX_MQ_ERR_ERROR,
      0,
      pTss->m_szBuf2K,
      pTss->m_szBuf2K)) {
    CWX_ERROR(("Failure to pack mq reply msg, err=%s", pTss->m_szBuf2K));
    m_pApp->noticeCloseConn(msg->event().getConnId());
    return 1;
  }
  reply(m_pApp, pBlock, msg->event().getConnId());
  return 1;
}

bool CwxProProxyRecvHandler::prepareUnzipBuf() {
  if (!m_unzipBuf) {
    m_uiBufLen = CWX_MQ_MAX_MSG_SIZE + 4096;
    if (m_uiBufLen < 1024 * 1024)
      m_uiBufLen = 1024 * 1024;
    m_unzipBuf = new unsigned char[m_uiBufLen];
  }
  return m_unzipBuf != NULL;
}

void CwxProProxyRecvHandler::reply(CwxProProxyApp* app, CwxMsgBlock* msg, CWX_UINT32 uiConnId) {
  msg->send_ctrl().setConnId(uiConnId);
  msg->send_ctrl().setSvrId(CwxProProxyApp::SVR_PRO_TYPE_RECV);
  msg->send_ctrl().setHostId(0);
  msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (0 != app->sendMsgByConn(msg)) {
    CWX_ERROR(("Failure to reply error msg."));
    CwxMsgBlockAlloc::free(msg);
    app->noticeCloseConn(uiConnId);
  }
}

int CwxProProxyRecvHandler::onConnClosed(CwxMsgBlock*& , CwxTss* ) {
  return 1;
}

int CwxProProxyRecvHandler::onTimeoutCheck(CwxMsgBlock*& , CwxTss* pThrEnv) {
  list<CwxTaskBoardTask*>  tasks;
  m_pApp->getTaskBoard().noticeCheckTimeout(pThrEnv, tasks);
  if (!tasks.empty()) {
    list<CwxTaskBoardTask*>::iterator iter = tasks.begin();
    while(iter != tasks.end()) {
      (*iter)->execute(pThrEnv);
      iter++;
    }
  }
  checkUnconndMq();
  return 1;
}

///接收zk的mq-master变更通知
int CwxProProxyRecvHandler::onUserEvent(CwxMsgBlock*& msg, CwxTss* pThrEnv) {
  ///CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
  if (EVENT_ZK_MASTER_EVENT == msg->event().getEvent()) { ///mq-mster发生变换
    map<string, CwxMqGroup>* now_group;
    memcpy(&now_group, msg->rd_ptr(), sizeof(&now_group));
    map<string, CwxMqGroupConn>::iterator conn_iter;
    map<string, CwxMqGroup>::iterator  group_iter;
    CWX_INT32 uiConnId;
    ///查找新增/变更的master-mq
    for(group_iter = now_group->begin(); group_iter != now_group->end(); group_iter++){
      conn_iter = m_mqGroup.find(group_iter->first);
      if ((conn_iter == m_mqGroup.end()) ||
          (conn_iter->second.m_zkGroup.m_strMasterId != group_iter->second.m_strMasterId ||
           conn_iter->second.m_zkGroup.m_strMasterHost != group_iter->second.m_strMasterHost ||
           conn_iter->second.m_zkGroup.m_unMasterPort != group_iter->second.m_unMasterPort)) { ///新增或变换master-mq
        if (conn_iter != m_mqGroup.end()) {
          CWX_INFO(("Group:%s master change from[%s:%s:%d] to [%s:%s:%d]", group_iter->first.c_str(),
            conn_iter->second.m_zkGroup.m_strMasterId.c_str(),
            conn_iter->second.m_zkGroup.m_strMasterHost.c_str(),
            conn_iter->second.m_zkGroup.m_unMasterPort,
            group_iter->second.m_strMasterId.c_str(),
            group_iter->second.m_strMasterHost.c_str(),
            group_iter->second.m_unMasterPort));
          if (conn_iter->second.m_uiMqConnId >= 0)
            m_pApp->noticeCloseConn(conn_iter->second.m_uiMqConnId);
        }
        if (group_iter->second.m_strMasterHost.length() &&
            group_iter->second.m_unMasterPort !=0) {
          uiConnId = m_pApp->noticeTcpConnect(CwxProProxyApp::SVR_PRO_TYPE_MQ,
              0,
              group_iter->second.m_strMasterHost.c_str(),
              group_iter->second.m_unMasterPort,
              false,
              1,
              2,
              NULL,
              m_pApp);
          if (uiConnId < 0) {
            CWX_ERROR(("Failure to connect mq[%s:%d]",
                group_iter->second.m_strMasterHost.c_str(),
                group_iter->second.m_unMasterPort));
          }
          CWX_INFO(("New mq group:%s, master-id:%s host:%s port:%d conn id :%d",
              group_iter->first.c_str(), group_iter->second.m_strMasterId.c_str(),
              group_iter->second.m_strMasterHost.c_str(),
              group_iter->second.m_unMasterPort, uiConnId));
          m_mqGroup[group_iter->first] = CwxMqGroupConn(group_iter->second, uiConnId);
        }
      }
    }
    ///查找被删除的group
    for(conn_iter = m_mqGroup.begin(); conn_iter != m_mqGroup.end();) {
      if (now_group->find(conn_iter->first) == now_group->end()) {
        CWX_INFO(("Group:%s master-id:%s, master-host:%s, master-port:%d, conn_id:%d need delete.",
            conn_iter->first.c_str(),
            conn_iter->second.m_zkGroup.m_strMasterId.c_str(),
            conn_iter->second.m_zkGroup.m_strMasterHost.c_str(),
            conn_iter->second.m_zkGroup.m_unMasterPort,
            conn_iter->second.m_uiMqConnId));
        if (conn_iter->second.m_uiMqConnId >= 0) m_pApp->noticeCloseConn(conn_iter->second.m_uiMqConnId);
        m_mqGroup.erase(conn_iter++);
      }else {
        conn_iter++;
      }
    }
    delete now_group;
  }else if (EVENT_ZK_TOPIC_CHANGE == msg->event().getEvent()){ ///mq-topic变化通知
    if (m_mqTopic) delete m_mqTopic;
    m_mqTopic = NULL;
    memcpy(&m_mqTopic, msg->rd_ptr(), sizeof(&m_mqTopic));
  }else{
    CWX_ERROR(("Recv unkown event type:%d", msg->event().getEvent()));
    return 0;
  }
  return 1;
}

void CwxProProxyRecvHandler::checkUnconndMq() {
  map<string, CwxMqGroupConn>::iterator conn_iter = m_mqGroup.begin();
  while (conn_iter != m_mqGroup.end()) {
    if (conn_iter->second.m_uiMqConnId < 0) {
      CWX_INFO(("Need reconnect group:%s, master-id:%s, master-host:%s, master-port:%d",
          conn_iter->first.c_str(),
          conn_iter->second.m_zkGroup.m_strMasterId.c_str(),
          conn_iter->second.m_zkGroup.m_strMasterHost.c_str(),
          conn_iter->second.m_zkGroup.m_unMasterPort));
      conn_iter->second.m_uiMqConnId = m_pApp->noticeTcpConnect(CwxProProxyApp::SVR_PRO_TYPE_MQ,
          0,
          conn_iter->second.m_zkGroup.m_strMasterHost.c_str(),
          conn_iter->second.m_zkGroup.m_unMasterPort,
          false,
          1,
          2,
          NULL,
          m_pApp);
      if (conn_iter->second.m_uiMqConnId < 0) {
        CWX_ERROR(("Failure to connect group:%s, master-id:%s, master-host:%s, master-port:%d",
            conn_iter->first.c_str(),
            conn_iter->second.m_zkGroup.m_strMasterId.c_str(),
            conn_iter->second.m_zkGroup.m_strMasterHost.c_str(),
            conn_iter->second.m_zkGroup.m_unMasterPort));
      }
    }
    conn_iter++;
  }
}
