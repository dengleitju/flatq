#ifndef __CWX_MQ_APP_H__
#define __CWX_MQ_APP_H__

#include "CwxMqMacro.h"
#include "CwxAppFramework.h"
#include "CwxBinLogMgr.h"
#include "CwxMqConfig.h"
#include "CwxMqTss.h"
#include "CwxMqPoco.h"
#include "CwxMqInnerDispHandler.h"
#include "CwxMqOuterDispHandler.h"
#include "CwxMqRecvHandler.h"
#include "CwxMqMasterHandler.h"
#include "CwxMqZkHandler.h"
#include "CwxThreadPool.h"

///应用信息
#define CWX_MQ_VERSION "3.0.0"
#define CWX_MQ_MODIFY_DATE "20140917153200"

///MQ服务的app对象
class CwxMqApp : public CwxAppFramework {
public:
  enum{
    //监控的buf空间大小
    MAX_MONITOR_REPLY_SIZE = 1024 * 1024,
    // 每个循环使用日志文件的MByte
    LOG_FILE_SIZE = 30,
    //可循环使用的日志文件数量
    LOG_FILE_NUM = 7,
    //消息接收的svr type
    SVR_TYPE_RECV = CwxAppFramework::SVR_TYPE_USER_START,
    //数据内部分发的svr type
    SVR_TYPE_INNER_DISP = CwxAppFramework::SVR_TYPE_USER_START + 2,
    //数据外部分发的svr type
    SVR_TYPE_OUTER_DISP = CwxAppFramework::SVR_TYPE_USER_START + 3,
    //从master接收数据的svr type
    SVR_TYPE_MASTER = CwxAppFramework::SVR_TYPE_USER_START + 4,
    //监控的服务类型
    SVR_TYPE_MONITOR = CwxAppFramework::SVR_TYPE_USER_START + 5,
    //zk的消息svr type
    SVR_TYPE_ZK = CwxAppFramework::SVR_TYPE_USER_START + 7
  };
  ///构造函数
  CwxMqApp();
  ///析构函数
  virtual ~CwxMqApp();
  ///重载初始化函数
  virtual int init(int argc, char** argv);
public:
  ///时钟响应函数
  virtual void onTime(CwxTimeValue const& current);
  ///signal响应函数
  virtual void onSignal(int signum);
  ///连接建立
  virtual int onConnCreated(CWX_UINT32 uiSvrId, ///svr id
      CWX_UINT32 uiHostId, ///host id
      CWX_HANDLE handle, ///连接handle
      bool& bSuspendListen ///是否suspend listen
      );
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
      bool& bSuspendConn);
  ///收到消息的响应函数
  virtual int onRecvMsg(CwxAppHandler4Msg& conn,
      bool& bSuspendConn);
public:
  ///计算机的时钟是否回调
  static bool isClockBack(CWX_UINT32& uiLastTime,
      CWX_UINT32 uiNow)
  {
    if (uiLastTime > uiNow + 1) {
      uiLastTime = uiNow;
      return true;
    }
    uiLastTime = uiNow;
    return false;
  }
  ///将当前的SID加1并返回，只有master才形成sid
  inline CWX_UINT64 nextSid() {
    return ++m_uiCurSid;
  }
  ///获取当前的sid
  inline CWX_UINT64 getCurSid() {
    return m_uiCurSid;
  }
  ///设置当前sid
  inline void setCurSid(CWX_UINT64 sid) {
    m_uiCurSid = sid;
  }
  ///返回当前最新sid的时间戳
  inline CWX_UINT32 getCurTimestamp() {
    return m_uiCurTimestamp;
  }
  ///设置当前最新sid的时间戳
  inline void setCurTimestamp(CWX_UINT32 timestamp) {
    m_uiCurTimestamp = timestamp;
  }
  ///返回当前start sid
  inline CWX_UINT64 getStartSid() {
    return m_ullStartSid;
  }
  ///设置binlogMgr的start sid
  inline void setStartSid(CWX_UINT64 sid) {
    m_ullStartSid = sid;
  }
  ///获取配置信息对象
  inline CwxMqConfig & getConfig() {
    return m_config;
  }
  ///获取binlog manager 对象指针
  inline CwxTopicMgr* getTopicMgr() {
    return m_pTopicMgr;
  }
  ///获取slave从master同步binlog的handler对象
  inline CwxMqMasterHandler* getMasterHandler() {
    return m_masterHandler;
  }
  ///获取rec的handle对象
  inline CwxMqRecvHandler* getRecvHanlder() {
    return m_recvHandler;
  }
  //获取zk的handler对象
  inline CwxMqZkHandler* getZkHandler() {
    return m_zkHandler;
  }
  ///获取当前的时间
  inline CWX_UINT32 getCurTime() const {
    return m_ttCurTime;
  }
  ///获取内部分发的channel
  CwxAppChannel* getInnerDispChannel() {
    return m_innerDispatchChannel;
  }
  ///获取外部分发channel
  CwxAppChannel* getOuterDispChannel() {
    return m_outerDispatchChannel;
  }
  ///获取zk的线程池
  inline CwxThreadPool* getZkThreadPool() {
    return m_zkThreadPool;
  }
  ///获取recv的线程池
  inline CwxThreadPool* getRecvThreadPool() {
    return m_recvThreadPool;
  }
  ///获取内部dispatch线程池
  inline CwxThreadPool* getInnerDispatchThreadPool() {
    return m_innerDispatchThreadPool;
  }
  ///获取外部dispatch线程池
  inline CwxThreadPool* getOuterDispatchThreadPool() {
    return m_outerDispatchThreadPool;
  }
  ///更新source的sid
  inline void updateSource(string topic, const CwxMqZkSource& source) {
    CwxWriteLockGuard<CwxRwLock> lock(&m_rwSourceLock);
    m_topicSource[topic][source.m_strSource] = source;
  }
  ///获取sourcesid:1:获取成功 -1:没有
  inline int getSouce(string topic, string source, CWX_UINT64& sid) {
    CwxWriteLockGuard<CwxRwLock> lock(&m_rwSourceLock);
    if(m_topicSource.find(topic) != m_topicSource.end()) {
      if (m_topicSource[topic].find(source) != m_topicSource[topic].end()) {
        sid = m_topicSource[topic][source].m_ullSid;
        return 1;
      }
    }
    return -1;
  }
  ///获取source信息
  inline int dumpSource(map<string, map<string, CwxMqZkSource> >& source) {
    source.clear();
    CwxWriteLockGuard<CwxRwLock> lock(&m_rwSourceLock);
    map<string, map<string, CwxMqZkSource> >::iterator iter = m_topicSource.begin();
    while (iter != m_topicSource.end()) {
      map<string, CwxMqZkSource>::iterator iter2 = iter->second.begin();
      for (;iter2 != iter->second.end(); iter2++) {
        source[iter->first][iter2->first] = iter2->second;
      }
      iter++;
    }
    return 0;
  }
protected:
  ///重载运行环境设置API
  virtual int initRunEnv();
  ///释放app资源
  virtual void destroy();
private:
  ///启动binlog管理器，-1：失败；0：成功
  int startBinLogMgr();
  ///启动网络，-1：失败；0：成功
  int startNetwork();
  ///返回监控信息
  CWX_UINT32 packMonitorInfo();
  ///stats命令，-1：因为错误关闭连接；0：不关闭连接
  int monitorStats(char const* buf,
      CWX_UINT32 uiDataLen,
      CwxAppHandler4Msg& conn);
  ///zk线程函数
  static void* zkThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg);
  ///内部分发channel的线程函数，arg为app对象
  static void* innerDispatchThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg);
  ///内部分发channel的队列消息函数。返回值：0：正常；-1：队列停止
  static int dealInnerDispatchThreadQueueMsg(CwxTss* tss,
      CwxMsgQueue* queue,
      CwxMqApp* app,
      CwxAppChannel*);
  ///外部分发channel的线程函数，arg为app对象
  static void* outerDispatchThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg);
  ///外部分发channel的队列消息函数。返回值：0：正常，-1：队列停止
  static int dealOuterDispatchThreadQueueMsg(CwxTss* tss,
      CwxMsgQueue* queue,
      CwxMqApp* app,
      CwxAppChannel*);
  ///设置master recv连接的属性
  static int setMasterRecvSockAttr(CWX_HANDLE handle,
      void* arg);
  ///设置slave dispatch连接的属性
  static int setDispatchSockAttr(CWX_HANDLE handle,
      void* arg);
  ///master模式时检测topic配置文件信息变化
  void checkTopicConfModify();
  ///加载topic配置文件信息
  int loadTopicChange(bool bForceLoad=false);
  ///保存sid到文件
  void saveMaxSid();
private:
  ///保存当前最大sid文件
  string                    m_strMaxSidFile;
  // 当前的sid
  CWX_UINT64                m_uiCurSid;
  // 最大sid的时间戳
  CWX_UINT32                m_uiCurTimestamp;
  // 最小sid
  CWX_UINT64                m_ullStartSid;
  //配置信息
  CwxMqConfig               m_config;
  //topic的binlog管理对象
  CwxTopicMgr*              m_pTopicMgr;
  //从master接收消息的handler
  CwxMqMasterHandler*       m_masterHandler;
  //接收消息的handler
  CwxMqRecvHandler*         m_recvHandler;
  ///zk消息的handler
  CwxMqZkHandler*           m_zkHandler;
  //zookeeper线程池对象
  CwxThreadPool*            m_zkThreadPool;
  //消息接收线程池对象
  CwxThreadPool*            m_recvThreadPool;
  //内部消息分发的线程池对象
  CwxThreadPool*            m_innerDispatchThreadPool;
  //内部消息分发的channel
  CwxAppChannel*            m_innerDispatchChannel;
  //外部消息分发线程池对象
  CwxThreadPool*            m_outerDispatchThreadPool;
  //外部消息分发channel
  CwxAppChannel*            m_outerDispatchChannel;
  //启动时间
  string                    m_strStartTime;
  //当前时间
  volatile CWX_UINT32       m_ttCurTime;
  ///上次检测topic配置文件时间
  CWX_UINT32                m_uiTopicFileModifyTime;
  ///错误信息
  char                      m_szErr2K[2048];
  //监控消息的回复buf
  char                      m_szBuf[MAX_MONITOR_REPLY_SIZE];
  //souce/sid对应Map锁
  CwxRwLock                 m_rwSourceLock;
  ///各topic下的source同步点信息
  map<string, map<string, CwxMqZkSource> >   m_topicSource; ///由于zk/dispatch线程都访问，故需要锁保护
};



#endif
