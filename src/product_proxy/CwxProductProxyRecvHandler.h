#ifndef __CWX_PRODUCT_PROXY_RECV_HANDLER_H__
#define __CWX_PRODUCT_PROXY_RECV_HANDLER_H__

#include "CwxCommander.h"
#include "CwxTss.h"
#include "CwxGlobalMacro.h"
#include "CwxMqTss.h"
#include "CwxMqMacro.h"
#include "CwxProductConfig.h"
#include "CwxProductProxyTask.h"
class CwxProProxyApp;

///代理mq消息的接收handler
class CwxProProxyRecvHandler : public CwxCmdOp {
public:
  ///构造函数
  CwxProProxyRecvHandler(CwxProProxyApp* pApp): m_pApp(pApp) {
    m_mqTopic = NULL;
  }
  ///析构函数
  virtual ~CwxProProxyRecvHandler() {
    if (m_mqTopic) delete m_mqTopic;
  }
public:
  ///处理mq消息的函数
  virtual int onRecvMsg(CwxMsgBlock*& msg,  CwxTss* pThrEnv);
  //处理连接关闭的消息
  virtual int onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  //超时检查的消息
  virtual int onTimeoutCheck(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  //用户自定义事件处理函数
  virtual int onUserEvent(CwxMsgBlock*& msg, CwxTss* pThrEnv);
private:
  ///准备解压buf
  bool prepareUnzipBuf();
  ///检测未连接的group-mq
  void checkUnconndMq();
public:
  ///回复代理的mq消息
  static void reply(CwxProProxyApp* app, CwxMsgBlock* msg, CWX_UINT32 uiConnId);
private:
  CwxProProxyApp*               m_pApp; ///<app对象
  map<string, CwxMqGroupConn>   m_mqGroup; ///<mq-group的连接对象
  map<string, CwxMqTopicGroup>*  m_mqTopic; ///<mq-topic的连接对象
  unsigned char*                m_unzipBuf; ///<解压buf
  CWX_UINT32                    m_uiBufLen; ///<解压buf长度
};

#endif
