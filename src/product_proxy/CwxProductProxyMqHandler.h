#ifndef __CWX_PRODUCT_PROXY_MQ_HANDLER_H__
#define __CWX_PRODUCT_PROXY_MQ_HANDLER_H__

#include "CwxCommander.h"
#include "CwxTss.h"
#include "CwxGlobalMacro.h"
#include "CwxMqTss.h"
#include "CwxMqMacro.h"
#include "CwxProductConfig.h"
#include "CwxProductProxyTask.h"
class CwxProProxyApp;

class CwxProProxyMqHandler : public CwxCmdOp {
public:
  ///构造函数
  CwxProProxyMqHandler(CwxProProxyApp* pApp): m_pApp(pApp) {
  }
  ///析构函数
  virtual ~CwxProProxyMqHandler() {
  }
public:
  ///mq消息返回的处理函数
  virtual int onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  virtual int onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  virtual int onEndSendMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  virtual int onFailSendMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
public:
  static int sendMq(CwxProProxyApp* app, CWX_UINT32 uiTaskId, CwxMsgBlock*& msg, CWX_INT32 uiConnId);

private:
  CwxProProxyApp*        m_pApp; ///<app对象
};

#endif
