#ifndef __CWX_SEND_PROXY_ZH_HANDLER_H__
#define __CWX_SEND_PROXY_ZH_HANDLER_H__

/**
 * 发送代理zk线程handler
 */

#include "CwxPackageReaderEx.h"
#include "CwxPackageWriterEx.h"
#include "CwxMsgBlock.h"
#include "CwxThreadPool.h"
#include "CwxZkLocker.h"
#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxProductConfig.h"
#include "CwxMqTss.h"
class CwxProProxyApp;



///zk事件处理handler
class CwxProProxyZkHandler {
public:
  enum{
    INIT_RETRY_INTERNAL = 10, ///<初始化重试间隔，为10s
    REFETCH_INTERNAL = 60, ///<重新获取时间间隔
    MAX_ZK_DATA_SIZE = 1024 * 1024 ///<最大的zk空间大小
  };
public:
  ///构造函数
  CwxProProxyZkHandler(CwxProProxyApp* app, string const& zkRoot, string const & zkServer);
  ///析构函数
  virtual ~CwxProProxyZkHandler();
public:
  ///初始化：0：成功 -1：失败
  int init();
  ///初始化zookeeper事件
  void doEvent(CwxTss* tss, CwxMsgBlock* msg, CWX_UINT32 uiLeftEvent);
  ///停止zookeeper监控
  void stop();
public:
  ///是否已经初始化
  bool isInit() const {
    return m_bInit;
  }
  ///是否已经连接zk
  bool isConnected() const {
    return m_bConnected;
  }
  ///是否已经认证
  bool isAuth() const {
    return m_bAuth;
  }
  ///是否正常
  bool isValid() const {
    return m_bValid;
  }
  ///获取错误信息
  void getErrMsg(string& strErrMsg) {
    CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
    strErrMsg = m_szErr2K;
  }
private:
  ///连接zk。0：成功，-1：失败
  int _connect();
  ///认证zk。1：成功；0：等待认证结果；-1：认证失败
  int _auth();
  ///初始化连接信息
  void _reset();
  ///初始化zk连接信息。0：成功；-1：失败
  int _init();
  //设置所有节点的watch。0：成功；-1：失败
  int _watch();
  ///timeout处理。0：成功；-1：失败
  int _dealTimeoutEvent(CwxTss* tss,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiLeftEvent);
  ///连接事件处理。0：成功；-1：失败
  int _dealConnectedEvent(CwxTss* tss,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiLeftEvent);
  ///mq-group发生变化事件处理。0：成功；-1：失败
  int _dealMqGroupEvent(CwxTss* tss,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiLeftEvent);
  ///mq-master发生变化事件处理。0：成功；-1：失败
  int _dealMasterMqEvent(CwxTss* tss,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiLeftEvent);
  ///topic-group发生变化事件处理。0：成功；-1：失败
  int _dealTopicEvent(CwxTss* tss,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiLeftEvent);
  ///解析mq的配置信息
  int _parseMqConf(string const& value,
      CwxMqGroup& host);
  ///检测未设置的mq-group节点
  int checkMqGroup();
  ///检测group-master
  int checkGroup(string group);
  ///检测未设置的group-topic节点
  int checkGroupTopic(string group);
  ///发送master-mq变更消息通知
  void noticeMasterMq();
  ///发送topic-group变更通知
  void noticeTopicChange();
private:
  ///zk的默认全局回调函数
  static void watcher(zhandle_t* zzh,
      int type,
      int state,
      const char* path,
      void* context);
  ///zk认证回调函数
  static void zkAuthCallback(int rc,
      const void* data);
  ///zk的mq-group回调函数
  static void watcherMqGroup(zhandle_t* zzh,
      int type,
      int state,
      const char* path,
      void* context);
  ///zk的master-mq回调函数
  static void watherMasterMq(zhandle_t* zzh,
      int type,
      int state,
      const char* path,
      void* context);
  ///zk的group-topic回调函数
  static void watcherMqTopic(zhandle_t* zzh,
      int type,
      int state,
      const char* path,
      void* context);
public:
  CwxProProxyApp*         m_pApp; ///<proxyApp对象
  ZkAdaptor*              m_zk; ///<zk对象
  volatile bool           m_bInit; ///<是否已经初始化
  volatile bool           m_bConnected; ///<是否已经建立连接
  volatile bool           m_bAuth; ///是否已经认证
  volatile bool           m_bValid; ///<是否已经处理正常状态
  clientid_t*             m_clientId; ///<client id
  CwxMutexLock            m_mutex; ///<数据保护锁
  string                  m_strZkRoot; ///<根路径
  string                  m_strZkServer; ///<zkServer
  CWX_UINT64              m_ullVersion; ///<配置变更版本号
  map<string/*group*/, CwxMqGroup>  m_group; ///<mq组列表
  map<string/*topic*/, CwxMqTopicGroup > m_topic; ///<topic-group对应表
  char                    m_szZkDataBuf[MAX_ZK_DATA_SIZE];
  char                    m_szErr2K[2048];
};


#endif
