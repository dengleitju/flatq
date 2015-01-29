#ifndef __CWX_MQ_RECV_HANDLER_H__
#define __CWX_MQ_RECV_HANDLER_H__

#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
class CwxMqApp;

///master/zk-master模式下接收消息handler
class CwxMqRecvHandler : public CwxCmdOp {
public:
  ///构造函数
  CwxMqRecvHandler(CwxMqApp* pApp) : m_pApp(pApp) {
    m_unzipBuf = NULL;
    m_uiBufLen = 0;
    m_bCanWrite = false;
  }
  ///析构函数
  virtual ~CwxMqRecvHandler();
public:
  ///连接建立后，需要维护链接上的数据分发
  virtual int onConnCreated(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  ///连接关闭后，需要清理环境
  virtual int onConnClosed(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  ///处理收到binlog的事件
  virtual int onRecvMsg(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  ///对于同步dispatch，需要检查同步的超时
  virtual int onTimeoutCheck(CwxMsgBlock*& msg, CwxTss* pThrEnv);
  ///用户自定义事件处理函数
  virtual int onUserEvent(CwxMsgBlock*& msg, CwxTss* pThrEnv);
public:
  ///是否可写
  bool isCanWrite() {
    return m_bCanWrite;
  }
  ///设置为可写
  void setCanWrite() {
    m_bCanWrite = true;
  }
private:
  ///-1:失败；0：成功
  int commit(char* szErr2K);
  //获取unzip的buf
  bool prepareUnzipBuf();
  ///mq master锁发生变化
  void configChange(CwxMqTss* pTss);
private:
  CwxMqApp*                  m_pApp;  ///<app对象
  unsigned char*             m_unzipBuf; ///<解压的buffer
  CWX_UINT32                 m_uiBufLen; ///<解压buffer的大小，其为trunk的20倍，最小为20M。
  volatile bool              m_bCanWrite; ///<是否为master
  map<string, CwxBinLogMgr*> m_binlogMgr; ///<recvHandler的BinlogMgr镜像
};

#endif
