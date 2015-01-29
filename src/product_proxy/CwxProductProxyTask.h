#ifndef __CWX_PRODUCT_PROXY_TASK_H__
#define __CWX_PRODUCT_PROXY_TASK_H__

#include "CwxTaskBoard.h"
#include "CwxTss.h"
#include "CwxGlobalMacro.h"
#include "CwxMqTss.h"
#include "CwxMqMacro.h"
class CwxProProxyApp;

///消息发送task
class CwxProProxyTask : public CwxTaskBoardTask {
public:
  enum
  {
    TASK_STATE_WAITING = TASK_STATE_USER
  };
public:
  ///构造函数
  CwxProProxyTask(CwxProProxyApp* pApp, CwxTaskBoard* pTaskBoard):CwxTaskBoardTask(pTaskBoard),m_pApp(pApp) {
    m_uiReplyConnId = 0;
    m_uiMsgTaskId = 0;
    m_sndMsg = NULL;
    m_bReplyTimeout = false;
    m_bFailSend = false;
    m_mqReply =NULL;
    m_uiMqConnId = 0;
  }
  ///析构函数
  virtual ~CwxProProxyTask() {
    if (m_sndMsg) CwxMsgBlockAlloc::free(m_sndMsg);
    if (m_mqReply) CwxMsgBlockAlloc::free(m_mqReply);
  }
public:
    /**
    @brief 通知Task已经超时
    @param [in] pThrEnv 调用线程的Thread-env
    @return void
    */
    virtual void noticeTimeout(CwxTss* pThrEnv);
    /**
    @brief 通知Task的收到一个数据包。
    @param [in] msg 收到的消息
    @param [in] pThrEnv 调用线程的Thread-env
    @param [out] bConnAppendMsg 收到消息的连接上，是否还有待接收的其他消息。true：是；false：没有
    @return void
    */
    virtual void noticeRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv, bool& bConnAppendMsg);
    /**
    @brief 通知Task往外发送的一个数据包发送失败。
    @param [in] msg 收到的消息
    @param [in] pThrEnv 调用线程的Thread-env
    @return void
    */
    virtual void noticeFailSendMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
    /**
    @brief 通知Task通过某条连接，发送了一个数据包。
    @param [in] msg 发送的数据包的信息
    @param [in] pThrEnv 调用线程的Thread-env
    @param [out] bConnAppendMsg 发送消息的连接上，是否有等待回复的消息。true：是；false：没有
    @return void
    */
    virtual void noticeEndSendMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv, bool& bConnAppendMsg);
    /**
    @brief 通知Task等待回复消息的一条连接关闭。
    @param [in] uiSvrId 关闭连接的SVR-ID
    @param [in] uiHostId 关闭连接的HOST-ID
    @param [in] uiConnId 关闭连接的CONN-ID
    @param [in] pThrEnv 调用线程的Thread-env
    @return void
    */
    virtual void noticeConnClosed(CWX_UINT32 uiSvrId, CWX_UINT32 uiHostId, CWX_UINT32 uiConnId, CwxTss* pThrEnv);
    /**
    @brief 激活Task。在Task启动前，Task有Task的创建线程所拥有。
    在启动前，Task可以接受自己的异步消息，但不能处理。
    此时有Taskboard的noticeActiveTask()接口调用的。
    @param [in] pThrEnv 调用线程的Thread-env
    @return 0：成功；-1：失败
    */
    virtual int noticeActive(CwxTss* pThrEnv);
  /**
  @brief 执行Task。在调用此API前，Task在Taskboard中不存在，也就是说对别的线程不可见。
  Task要么是刚创建状态，要么是完成了前一个阶段的处理，处于完成状态。
  通过此接口，由Task自己控制自己的step的跳转而外界无需关系Task的类型及处理过程。
  @param [in] pTaskBoard 管理Task的Taskboard
  @param [in] pThrEnv 调用线程的Thread-env
  @return void
  */
  virtual void execute(CwxTss* pThrEnv);
private:
    void reply(CwxTss* pThrEnv);
public:
    CWX_UINT32        m_uiReplyConnId; ///<回复的连接ID
    CWX_UINT32        m_uiMsgTaskId; ///<接收到消息的TaskId
    string            m_strMqTopic; ///<发送的消息topic信息
    string            m_strMqGroup; ///<发送的mq-group
    CWX_INT32         m_uiMqConnId; ///<mq-group对应的conn_id
    CwxMsgBlock*      m_sndMsg; ///<发送的mq消息
private:
    bool              m_bReplyTimeout; ///<是否回复超时
    bool              m_bFailSend; ///<是否发送失败
    CwxMsgBlock*      m_mqReply;
    CwxProProxyApp*   m_pApp; ///<app对象
};


#endif
