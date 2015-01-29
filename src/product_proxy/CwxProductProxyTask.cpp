#include "CwxProductProxyTask.h"
#include "CwxProductProxyApp.h"

void CwxProProxyTask::noticeTimeout(CwxTss* )
{
  setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
  m_bReplyTimeout = true;
}

void CwxProProxyTask::noticeRecvMsg(CwxMsgBlock*& msg, CwxTss* , bool& )
{
  m_mqReply = msg;
  msg = NULL;
  setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
}

void CwxProProxyTask::noticeFailSendMsg(CwxMsgBlock*& , CwxTss* )
{
  m_bFailSend = true;
  setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
}

void CwxProProxyTask::noticeEndSendMsg(CwxMsgBlock*& , CwxTss* , bool& )
{
}

void CwxProProxyTask::noticeConnClosed(CWX_UINT32 , CWX_UINT32 , CWX_UINT32 , CwxTss* )
{
  m_bFailSend = true;
  setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
}

int CwxProProxyTask::noticeActive(CwxTss*  )
{
  setTaskState(TASK_STATE_WAITING);
  assert(m_uiMqConnId > 0);
  if (0 != CwxProProxyMqHandler::sendMq(m_pApp, getTaskId(), m_sndMsg, m_uiMqConnId))
  {
    m_bFailSend = true;
    setTaskState(CwxTaskBoardTask::TASK_STATE_FINISH);
  }
  return 0;
}

void CwxProProxyTask::execute(CwxTss* pThrEnv)
{
  if (CwxTaskBoardTask::TASK_STATE_INIT == getTaskState())
  {
    m_bReplyTimeout = false;
    m_bFailSend = false;
    CWX_UINT64 timeStamp = m_pApp->getConfig().getCmmon().m_uiSendTimeout;
    timeStamp *= 1000000;
    timeStamp += CwxDate::getTimestamp();
    this->setTimeoutValue(timeStamp);
    getTaskBoard()->noticeActiveTask(this, pThrEnv);
  }
  if (CwxTaskBoardTask::TASK_STATE_FINISH == getTaskState())
  {
    reply(pThrEnv);
    delete this;
  }
 }

void CwxProProxyTask::reply(CwxTss* pThrEnv)
{
  CwxMqTss* pTss = (CwxMqTss*)pThrEnv;
  CwxMsgBlock* replyMsg = NULL;
  int ret = CWX_MQ_ERR_SUCCESS;
  CWX_UINT64 ullSid = 0;
  char const* szErrMsg = NULL;
  if (m_mqReply)
  {
    if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::parseRecvDataReply(pTss->m_pReader,
        m_mqReply,
        ret,
        ullSid,
        szErrMsg,
        pTss->m_szBuf2K))
    {
      CWX_ERROR(("Failure to parse mq's reply, err:%s", pTss->m_szBuf2K));
      ret = CWX_MQ_ERR_ERROR;
      szErrMsg = "Failure to parse mq's rely";
    }
  }
  else if (m_bReplyTimeout)
  {
    ret = CWX_MQ_ERR_ERROR;
    szErrMsg = "Mq reply is timeout";
  }
  else if (m_bFailSend)
  {
    ret = CWX_MQ_ERR_ERROR;
    szErrMsg = "Mq is not available";
  }
  else
  {
    CWX_ASSERT(0);
    ret = CWX_MQ_ERR_ERROR;
    szErrMsg = "Unknown error";
  }
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packProxyRecvDataReply(pTss->m_pWriter,
      replyMsg,
      m_uiMsgTaskId,
      ret,
      ullSid,
      szErrMsg,
      pTss->m_szBuf2K
      ))
  {
    CWX_ERROR(("Failure to pack mq reply, err:%s", pTss->m_szBuf2K));
    m_pApp->noticeCloseConn(m_uiReplyConnId);
  }else {
    CwxProProxyRecvHandler::reply(m_pApp, replyMsg, m_uiReplyConnId);
  }
}

