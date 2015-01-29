#ifndef __CWX_PRODUCT_PROXY_APP_H__
#define __CWX_PRODUCT_PROXY_APP_H__

#include "CwxMqMacro.h"
#include "CwxAppFramework.h"
#include "CwxProductConfig.h"
#include "CwxProductProxyZkHandler.h"
#include "CwxProductProxyMqHandler.h"
#include "CwxProductProxyRecvHandler.h"
#include "CwxThreadPool.h"

///一个用信息
#define CWX_PRODUCT_PROXY_VERSION "1.0.0"
#define CWX_PRODUCT_PROXY_MODIFY "201410270923"

///mq发送代理app对象
class CwxProProxyApp : public CwxAppFramework{
public:
  enum{
    //监控的buf空间大小
    MAX_MONITOR_REPLY_SIZE = 1024 * 1024,
    // 每个循环使用日志文件的MByte
    LOG_FILE_SIZE = 30,
    //可循环使用的日志文件数量
    LOG_FILE_NUM = 7,
    //消息接收的svr type
    SVR_PRO_TYPE_RECV = CwxAppFramework::SVR_TYPE_USER_START,
    ///mq的消息svr type
    SVR_PRO_TYPE_MQ = CwxAppFramework::SVR_TYPE_USER_START + 1,
    //监控的服务类型
    SVR_PRO_TYPE_MONITOR = CwxAppFramework::SVR_TYPE_USER_START + 3,
    //zk的消息svr type
    SVR_PRO_TYPE_ZK = CwxAppFramework::SVR_TYPE_USER_START + 4
  };
  ///构造函数
  CwxProProxyApp();
  ///析构函数
  virtual ~CwxProProxyApp();
  ///重载初始化函数
  virtual int init(int argc, char** argv);
public:
  ///时钟响应函数
  virtual void onTime(CwxTimeValue const& current);
  ///signal响应函数
  virtual void onSignal(int signum);
  ///连接建立
  virtual int onConnCreated(CwxAppHandler4Msg& conn,
      bool& bSuspendConn,
      bool& bSuspendListen);
  ///连接关闭
  virtual int onConnClosed(CwxAppHandler4Msg& msg,
      CwxAppHandler4Msg& conn,
      CwxMsgHead const& header,
      bool& bSuspendConn);
  ///收到消息的响应函数
  virtual int onRecvMsg(CwxMsgBlock* msg,
      CwxAppHandler4Msg& conn,
      CwxMsgHead const& header,
      bool& bSuspendConn);
  ///消息发送失败
  virtual void onFailSendMsg(CwxMsgBlock*& msg);
public:
  ///计算机时钟是否回调
  static bool isClockBack(CWX_UINT32& uiLastTime,
      CWX_UINT32 uiNow) {
    if (uiLastTime > uiNow + 1) {
      uiLastTime = uiNow;
      return true;
    }
    uiLastTime = uiNow;
    return false;
  }
  ///获取配置文件
  inline CwxProductConfig& getConfig() {
    return m_config;
  }
  ///获取zk的handler
  inline CwxProProxyZkHandler* getZkhandler() {
    return m_zkHandler;
  }
  ///获取zk线程池
  inline CwxThreadPool* getZkThreadPool() {
    return m_zkThreadPool;
  }
  ///获取mq线程池
  inline CwxThreadPool* getMqThreadPool() {
    return m_mqThreadPool;
  }
  ///获取下一个taskId
  CWX_UINT32 getNextTaskId()
  {
    CwxMutexGuard<CwxMutexLock>  lock(&m_taskIdLock);
    m_uiTaskId++;
    if (!m_uiTaskId) m_uiTaskId = 1;
    return m_uiTaskId;
  }
public:
  ///设置mq连接socket属性
  static int setMqSockAttr(CWX_HANDLE handle, void* arg);
  ///设置recv连接socket属性
  static int setRecvSockAttr(CWX_HANDLE handle, void* arg);
protected:
  ///重载运行环境设置API
  virtual int initRunEnv();
  ///释放app资源
  virtual void destroy();
private:
  ///zk线程函数
  static void* zkThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg);
private:
  ///配置信息
  CwxProductConfig              m_config;
  ///mq消息handler
  CwxProProxyMqHandler*         m_mqHandler;
  ///recv消息handler
  CwxProProxyRecvHandler*       m_recvHandler;
  ///消息处理线程
  CwxThreadPool*                m_mqThreadPool;
  ///zk消息的handler
  CwxProProxyZkHandler*         m_zkHandler;
  ///zookeeper线程池对象
  CwxThreadPool*                m_zkThreadPool;
  ///启动时间
  string                        m_strStartTime;
  ///当前时间
  volatile CWX_UINT32           m_ttCurTime;
  ///taskId的保护锁
  CwxMutexLock                  m_taskIdLock;
  ///发送给mq的消息taskid
  CWX_UINT32                    m_uiTaskId;
  ///错误信息
  char                          m_szErr2K[2048];
  ///监控消息的回复buf
  char                          m_szBuf[MAX_MONITOR_REPLY_SIZE];
};

#endif
