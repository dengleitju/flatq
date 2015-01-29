#ifndef __CWX_SUB_PROXY_APP_H__
#define __CWX_SUB_PROXY_APP_H__

#include "CwxMqMacro.h"
#include "CwxAppFramework.h"
#include "CwxSubProxyConfig.h"
#include "CwxThreadPool.h"
#include "CwxSubProxyZkHandler.h"
#include "CwxSubProxySyncHandler.h"
#include "CwxSubProxyConfig.h"

///一个用信息
#define CWX_SUB_PROXY_VERSION "1.0.0"
#define CWX_SUB_PROXY_MODIFY "201411030923"

///mq订阅代理app对象
class CwxSubProxyApp : public CwxAppFramework {
public:
  enum{
    //监控的buf空间大小
    MAX_MONITOR_REPLY_SIZE = 1024 * 1024,
    // 每个循环使用日志文件的MByte
    LOG_FILE_SIZE = 30,
    //可循环使用的日志文件数量
    LOG_FILE_NUM = 7,
    //消息接收的svr type
    SVR_SUB_TYPE_RECV = CwxAppFramework::SVR_TYPE_USER_START,
    ///mq的消息svr type
    SVR_SUB_TYPE_SYNC = CwxAppFramework::SVR_TYPE_USER_START + 1,
    //监控的服务类型
    SVR_SUB_TYPE_MONITOR = CwxAppFramework::SVR_TYPE_USER_START + 3,
    //zk的消息svr type
    SVR_SUB_TYPE_ZK = CwxAppFramework::SVR_TYPE_USER_START + 4,
    // event类型的定义
    // sync host信息改变的消息
    EVENT_TYPE_SYNC_CHANGE = CwxEventInfo::SYS_EVENT_NUM + 1
  };
  ///构造函数
  CwxSubProxyApp();
  ///析构函数
  virtual ~CwxSubProxyApp();
  ///重载初始化函数
  virtual int init(int argc, char** argv);
public:
  ///时钟响应函数
  virtual void onTime(CwxTimeValue const& current);
  ///signal响应函数
  virtual void onSignal(int signum);
  ///连接建立
  virtual int onConnCreated(CwxAppHandler4Msg& conn, // 连接对象
    bool& bSuspendConn, // 是否suspend 连接消息的接收
    bool& bSuspendListen // 是否suspend 新连接的监听
    );
  ///连接关闭
  virtual int onConnClosed(CwxAppHandler4Msg& conn);
  ///收到消息的响应函数
  virtual int onRecvMsg(CwxMsgBlock* msg,
      CwxAppHandler4Msg& conn,
      CwxMsgHead const& header,
      bool&);
public:
  ///计算机的时钟是否回调
  static bool isClockBack(CWX_UINT32& uiLastTime,
    CWX_UINT32 uiNow) {
      if (uiLastTime > uiNow + 1) {
        uiLastTime = uiNow;
        return true;
      }
      uiLastTime = uiNow;
      return false;
  }
  ///启动监听端口
  int startNetwork();
  ///设置最新的mq-group信息
  void setNewMqGroup(map<string, CwxZkMqGroup>* group) {
    CWX_ASSERT(group);
    CwxMutexGuard<CwxMutexLock> lock(&m_mqMutex);
    if (m_newMqGroup) delete m_newMqGroup;
    m_newMqGroup = group;
  }
  ///获取配置信息
  CwxSubConfig const& getConfig() const {
    return m_config;
  }
  ///获取zk的消息handler
  CwxSubProxyZkHandler* getZkhandler() {
    return m_zkHandler;
  }
  ///获取zk线程池对象
  CwxThreadPool* getZkThreadPool() {
    return m_zkThreadPool;
  }
  ///检查同步信息变化
  void checkSyncHostModify();
  ///处理消息
  int recvMsg(CwxMsgBlock* msg, CwxAppHandler4Msg& conn);
  ///通知同步信息变化
  int noticeSyncHost(string group, CwxHostInfo const& hostInfo);
  ///获取当前时间
  inline CWX_UINT32 getCurTime() const{
    return m_ttCurTime;
  }
protected:
  ///重载运行环境设置API
  virtual int initRunEnv();
  ///释放app资源
  virtual void destroy();
private:
  ///zk线程函数
  static void* zkThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg);
  ///sync线程函数
  static void* syncThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg);
  ///sync消息处理函数
  static int dealSyncThreadMsg(CwxMsgQueue* queue, CwxSubSyncSession* pSession, CwxAppChannel* );
  ///设置recv的socket属性
  static int setRecvSockAttr(CWX_HANDLE handle, void* arg);
private:
  ///启动同步
  int startSync(char const* szGroup,
      char const* szTopic,
      char const* szSource,
      CWX_UINT64 ullSid,
      CWX_UINT32 uiChunkSize,
      bool bZip,
      bool bNewly,
      CWX_INT32 uiConnId);
  ///停止某个连接的同步
  int stopSync(CWX_INT32 uiConnId);
  ///发送client,同步关闭消息
  void replyError(CWX_INT32 uiConnId);
private:
  ///tss对象
  CwxMqTss*                        m_tss;
  ///conn对同步对象map
  map<CWX_INT32, CwxSubSyncSession*> m_syncs;
  ///配置信息
  CwxSubConfig                     m_config;
  ///zk线程的handler
  CwxSubProxyZkHandler*            m_zkHandler;
  ///zk线程池对象
  CwxThreadPool*                   m_zkThreadPool;
  // 当前的host id
  CWX_UINT32                       m_uiCurHostId;
  ///mq-group信息
  map<string, CwxZkMqGroup>        m_group;
  ///mq-group锁
  CwxMutexLock                     m_mqMutex;
  ///最新的mq-group表
  map<string, CwxZkMqGroup>*       m_newMqGroup;
  ///启动时间
  string                           m_strStartTime;
  ///当前时间
  volatile CWX_UINT32              m_ttCurTime;
};

#endif
