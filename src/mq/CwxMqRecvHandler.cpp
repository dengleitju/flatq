#include "CwxMqRecvHandler.h"
#include "CwxMqApp.h"
#include "CwxZlib.h"

CwxMqRecvHandler::~CwxMqRecvHandler() {
  if (m_unzipBuf) delete[] m_unzipBuf;
  if (m_binlogMgr.size()) {
    for(map<string, CwxBinLogMgr*>::iterator iter = m_binlogMgr.begin(); iter != m_binlogMgr.end(); iter++) {
      m_pApp->getTopicMgr()->freeBinlogMgrByTopic(iter->first, iter->second);
    }
  }
}

int CwxMqRecvHandler::onConnCreated(CwxMsgBlock*& , CwxTss*) {
  return 1;
}

int CwxMqRecvHandler::onConnClosed(CwxMsgBlock*& , CwxTss*) {
  return 1;
}

///用户自定义事件处理函数
int CwxMqRecvHandler::onUserEvent(CwxMsgBlock*& msg, CwxTss* pThrEnv) {
  CWX_ASSERT(m_pApp->getConfig().getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_ZK);
  CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
  if (EVENT_ZK_LOCK_CHANGE == msg->event().getEvent()) { ///master发生切换
    CwxMqZkLock* pLock = NULL;
    memcpy(&pLock, msg->rd_ptr(), sizeof(&pLock));
    if (pTss->m_pZkLock) {
      if (pTss->m_pZkLock->m_ullVersion > pLock->m_ullVersion) { ///采用旧版本
        delete pLock;
      }else { ///采用新版本
        delete pTss->m_pZkLock;
        pTss->m_pZkLock = pLock;
      }
    }else { ///采用新版本
      pTss->m_pZkLock = pLock;
    }
    CWX_INFO(("CwxMqRecvHandler: lock changed.master[%s:%s:%d], prev[%s:%s:%d]",
        pTss->m_pZkLock->m_strMaster.c_str(), pTss->m_pZkLock->m_strMasterInnerDispHost.c_str(),
        pTss->m_pZkLock->m_unMasterInnerDispPort, pTss->m_pZkLock->m_strPrev.c_str(),
        pTss->m_pZkLock->m_strPrevInnerDispHost.c_str(), pTss->m_pZkLock->m_unPrevInnerDispPort));
    configChange(pTss);
  }else {
    CWX_ERROR(("CwxMqRecvHandler: unknown event type:%u", msg->event().getEvent()));
    return 0;
  }

  return 1;
}

void CwxMqRecvHandler::configChange(CwxMqTss* pTss) {
  if (pTss->isMaster()) {
    if (!m_bCanWrite) {
      CWX_UINT64 ullSid = m_pApp->getCurSid();
      ullSid += CWX_MQ_MASTER_SWITCH_SID_INC;
      m_pApp->setCurSid(ullSid); ///sid跳过一段范围
      m_bCanWrite = true;
    }
  }else {
    m_bCanWrite = false;
  }
  m_pApp->getMasterHandler()->configChange(pTss);
}

///echo请求的处理函数
int CwxMqRecvHandler::onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv) {
  CwxMqTss* pTss = (CwxMqTss*) pThrEnv;
  if (!m_bCanWrite) {
    CWX_ERROR(("I am not master, can't recv msg so close it."));
    m_pApp->noticeCloseConn(msg->event().getConnId());
    return 1;
  }
  int iRet = CWX_MQ_ERR_SUCCESS;
  CWX_UINT64 ullSid = 0;
  do {
    ///binlog数据接收消息
    if (CwxMqPoco::MSG_TYPE_RECV_DATA == msg->event().getMsgHeader().getMsgType()) {
      CwxKeyValueItem const* pData;
      char const* szTopic;
      if (!msg) {
        strcpy(pTss->m_szBuf2K, "No data.");
        CWX_DEBUG((pTss->m_szBuf2K));
        iRet = CWX_MQ_ERR_ERROR;
        break;
      }
      unsigned long ulUnzipLen = 0;
      bool bZip = msg->event().getMsgHeader().isAttr(CwxMsgHead::ATTR_COMPRESS);
      //判断是否压缩数据
      if (bZip) { //压缩数据，需要解压
        //首先准备解压的buf
        if (!prepareUnzipBuf()) {
          iRet = CWX_MQ_ERR_ERROR;
          CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
            "Failure to prepare unzip buf, size:%u", m_uiBufLen);
          CWX_ERROR((pTss->m_szBuf2K));
          break;
        }
        ulUnzipLen = m_uiBufLen;
        //解压
        if (!CwxZlib::unzip(m_unzipBuf, ulUnzipLen,
          (const unsigned char*) msg->rd_ptr(), msg->length())) {
            iRet = CWX_MQ_ERR_ERROR;
            CwxCommon::snprintf(pTss->m_szBuf2K, 2047,
              "Failure to unzip recv msg, msg size:%u, buf size:%u",
              msg->length(), m_uiBufLen);
            CWX_ERROR((pTss->m_szBuf2K));
            break;
        }
      }
      if (CWX_MQ_ERR_SUCCESS != (iRet = CwxMqPoco::parseRecvData(pTss->m_pReader,
        bZip ? (char const*) m_unzipBuf : msg->rd_ptr(),
        bZip ? ulUnzipLen : msg->length(),
        pData,
        szTopic,
        pTss->m_szBuf2K)))
      {
        iRet = CWX_MQ_ERR_ERROR;
        //如果是无效数据，返回
        CWX_ERROR(("Failure to parse the recieve msg, err=%s", pTss->m_szBuf2K));
        break;
      }
      CwxBinLogMgr* binlogMgr = NULL;
      map<string, CwxBinLogMgr*>::iterator iter = m_binlogMgr.find(szTopic);
      if (iter == m_binlogMgr.end()) {
        binlogMgr = m_pApp->getTopicMgr()->getBinlogMgrByTopic(szTopic, true);
        if (!binlogMgr){
          CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid topic:%s", szTopic);
          CWX_ERROR((pTss->m_szBuf2K));
          iRet = CWX_MQ_ERR_ERROR;
          break;
        }
        m_binlogMgr[szTopic] = binlogMgr;
      }else {
        binlogMgr = iter->second;
      }
      if (binlogMgr->getZkTopicState() != CWX_MQ_TOPIC_NORMAL) {
        CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invlaid topic : %s.", szTopic);
        CWX_ERROR((pTss->m_szBuf2K));
        iRet = CWX_MQ_ERR_ERROR;
        ///删除binlogMgr引用
        m_binlogMgr.erase(szTopic);
        m_pApp->getTopicMgr()->freeBinlogMgrByTopic(szTopic, binlogMgr);
        break;
      }
      pTss->m_pWriter->beginPack();
      pTss->m_pWriter->addKeyValue(CWX_MQ_D,
        pData->m_szData,
        pData->m_uiDataLen,
        pData->m_bKeyValue);
      pTss->m_pWriter->pack();
      if (m_pApp->getCurSid() == 0) {
        m_pApp->setCurSid(CWX_MQ_MAX_BINLOG_FLUSH_COUNT + 1);
      }
      ullSid = m_pApp->nextSid();
      CWX_UINT32 timestamp = time(NULL);
      if (0 != binlogMgr->append(ullSid, timestamp,
        0,
        pTss->m_pWriter->getMsg(),
        pTss->m_pWriter->getMsgSize(),
        pTss->m_szBuf2K))
      {
        CWX_ERROR((pTss->m_szBuf2K));
        iRet = CWX_MQ_ERR_ERROR;
        break;
      }
      m_pApp->setCurTimestamp(timestamp);
      if (m_pApp->getStartSid() == 0) {
        m_pApp->setStartSid(ullSid);
      }
    } else {
      CwxCommon::snprintf(pTss->m_szBuf2K, 2047, "Invalid msg type:%u",
        msg->event().getMsgHeader().getMsgType());
      CWX_ERROR((pTss->m_szBuf2K));
      iRet = CWX_MQ_ERR_ERROR;
      break;
    }
  } while (0);
  CwxMsgBlock* pBlock = NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packRecvDataReply(pTss->m_pWriter,
    pBlock,
    msg->event().getMsgHeader().getTaskId(),
    iRet,
    ullSid,
    pTss->m_szBuf2K,
    pTss->m_szBuf2K))
  {
    CWX_ERROR(("Failure to pack mq reply msg, err=%s", pTss->m_szBuf2K));
    m_pApp->noticeCloseConn(msg->event().getConnId());
    return 1;
  }
  pBlock->send_ctrl().setConnId(msg->event().getConnId());
  pBlock->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_RECV);
  pBlock->send_ctrl().setHostId(0);
  pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (0 != m_pApp->sendMsgByConn(pBlock)) {
    CWX_ERROR(("Failure to reply error msg"));
    CwxMsgBlockAlloc::free(pBlock);
    m_pApp->noticeCloseConn(msg->event().getConnId());
  }
  return 1;
}

///超时检查1.检查删除的binlogMgr释放引用。2.在zk-slave/slave模式下调用masterHandler>timecheck()
int CwxMqRecvHandler::onTimeoutCheck(CwxMsgBlock*& , CwxTss* pThrEnv) {
  CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
  if (m_pApp->getConfig().getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_ZK) {
    if (!m_bCanWrite) { ///如果不是master,则需要同步，调用同步方法的timecheck
      m_pApp->getMasterHandler()->timecheck(pTss);
    }
  }else if (m_pApp->getConfig().getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_SLAVE) {
    ///为slave模式，则需要调用同步方法的timecheck
    m_pApp->getMasterHandler()->timecheck(pTss);
  }
  m_pApp->getTopicMgr()->timeout(m_pApp->getCurTime());
  if (m_binlogMgr.size()){///将标记为删除状态的binlogMgr移除
    map<string, CwxBinLogMgr*>::iterator iter = m_binlogMgr.begin();
    while(iter != m_binlogMgr.end()) {
      if (iter->second->getZkTopicState() == CWX_MQ_TOPIC_DELETE) {
        string strTopic = iter->first;
        CwxBinLogMgr* binlogMgr = iter->second;
        m_binlogMgr.erase(iter++);
        m_pApp->getTopicMgr()->freeBinlogMgrByTopic(strTopic, binlogMgr);
      }else {
        iter++;
      }
    }
  }
  return 1;
}

int CwxMqRecvHandler::commit(char* szErr2K) {
  int iRet = 0;
  CWX_INFO(("Begin flush bin log......."));
  m_pApp->getTopicMgr()->commit("", false, szErr2K);
  CWX_INFO(("End flush bin log......."));
  return iRet;
}

bool CwxMqRecvHandler::prepareUnzipBuf() {
  if (!m_unzipBuf) {
    m_uiBufLen = CWX_MQ_MAX_MSG_SIZE + 4096;
    if (m_uiBufLen < 1024 * 1024)
      m_uiBufLen = 1024 * 1024;
    m_unzipBuf = new unsigned char[m_uiBufLen];
  }
  return m_unzipBuf != NULL;
}
