#ifndef __CWX_MQ_ZK_HANDLER_H__
#define __CWX_MQ_ZK_HANDLER_H__

#include "CwxPackageReaderEx.h"
#include "CwxPackageWriterEx.h"
#include "CwxMsgBlock.h"
#include "CwxThreadPool.h"
#include "CwxZkLocker.h"
#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxMqConfig.h"
class CwxMqApp;

//zk事件处理handle
class CwxMqZkHandler {
public:
  enum{
    INIT_RETRY_INTERNAL = 10, ///<初始化重试间隔，为10s
    REFETCH_INTERNAL = 60, ///<重新获取时间间隔
    MAX_ZK_DATA_SIZE = 1024 * 1024 ///<最大的zk空间大小
  };
public:
  //构造函数
  CwxMqZkHandler(CwxMqApp* app);
  //析构函数
  virtual ~CwxMqZkHandler();
public:
  //初始化：0：成功；-1：失败
  int init();
  //初始化zookeeper事件
  void doEvent(CwxTss* tss, CwxMsgBlock*& msg, CWX_UINT32 uiLeftEvent);
  //停止zookeeper监控
  void stop();
public:
  ///是否已经初始化
  bool isInit() const {
    return m_bInit;
  }
  //是否已经连接zk
  bool isConnected() const {
    return m_bConnected;
  }
  //是否已经认证
  bool isAuth() const {
    return m_bAuth;
  }
  //是否正常
  bool isValid() const {
    return m_bValid;
  }
  //是否为master
  bool isMaster() {
    CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
    return _isMaster();
  }
  //获取配置信息
  void getConf(CwxMqConfigZk& conf) {
    CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
    conf = m_conf;
  }
  //获取锁信息
  void getLockInfo(CwxMqZkLock& zkLock) {
    CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
    zkLock = m_lock;
  }
  //获取错误信息
  void getErrMsg(string& strErrMsg) {
    CwxMutexGuard<CwxMutexLock> lock(&m_mutex);
    strErrMsg = m_szErr2K;
  }
private:
  //连接zk。0：成功；-1：失败
  int _connect();
  //认证zk。1：成功；0：等待认证结果 ；-1：认证失败
  int _auth();
  //初始化连接信息
  void _reset();
  //加锁。0：成功；-1：失败
  int _lock();
  //获取锁的信息。0：成功；-1：失败
  int _loadLockInfo(CwxTss* tss);
  //是否为master。true:是；false:否
  inline bool _isMaster() const {
    if (m_zkLocker) return m_zkLocker->isLocked();
    return false;
  }
  //初始化zk连接信息。0：成功；-1：失败
  int _init();
  //设置host信息。0：成功；-1：失败
  int _setMqInfo();
  //设置master信息。0：成功；-1：失败
  int _setMasterInfo();
  //连接成功时获取topic下的source信息
  int _processTopicInfo();
  //获取host的数据同步信息。0：成功；-1：失败
  int _getHostInfo(char const* szHost, //获取的主机标志
      CWX_UINT64& ullSid, ///获取主机当前的sid
      CWX_UINT32& uiTimeStamp ///获取最小的sid的时间戳
      );
  //获取master的信息。0：成功；-1：失败
  int _getMasterInfo(string& strMaster, //master标志
      CWX_UINT64& ullStartSid, ///master的start sid
      CWX_UINT64& ullMaxSid, ///master的max sid
      string& strTimestamp ///master的更新时间
      );
  ///通知配置变化
  void _noticeTopicChange(map<string, CWX_UINT8>* topics);
  //timeout处理。0：成功；-1：失败
  int _dealTimeoutEvent(CwxTss* tss, //<tss
      CwxMsgBlock*& msg, ///消息
      CWX_UINT32 uiLeftEvent///消息队列中滞留的消息数量
      );
  //连接事件处理。0：成功；-1：失败
  int _dealConnectedEvent(CwxTss* tss,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiLeftEvent
      );
  ///获取mq实例节点的分发端口信息
  int _getMqDispatchHost(string strNode,
      string& innter_ip,
      CWX_UINT16& innter_port
      );
  ///创建根节点信息
  int _createRootNode();
  //通知锁变化
  void _noticeLockChange();
  ///设置master信息
  int _setSourceInfo();
  ///获取source信息,zk连接成功之后调用，获取所有的source信息
  int _loadSourceInfo();
private:
  ///内部node变更watch回调函数
  static void watcher(zhandle_t* zzh,
      int type,
      int state,
      const char* path,
      void* context);
  //zk认证回调函数
  static void zkAuthCallback(int rc,
      const void* data);
  //锁的回调函数
  static void lock_complete(bool bLock ,//是否lock
      void* context
      );
private:
  CwxMqApp*               m_pApp; ///<mq app对象
  ZkAdaptor*              m_zk; ///<zk对象
  ZkLocker*               m_zkLocker; ///<locker对象
  volatile bool           m_bInit; ///<是否已经初始化
  volatile bool           m_bConnected; ///<是否已经建立连接
  volatile bool           m_bAuth; ///是否已经认证
  volatile bool           m_bValid; ///<是否已经处理正常状态
  clientid_t*             m_clientId; ///<client id
  CwxMqConfigZk           m_conf; ///<配置文件
  CwxMqZkLock             m_lock; ///<锁信息
  string                  m_strZkGroupNode; ///<group节点
  string                  m_strZkMqNode; ///<mq的节点
  string                  m_strZkMasterMq; ///<master-mq节点
  string                  m_strZkTopicNode; ///<topic节点
  string                  m_strZkDelTopicNode; ///<已经删除的topic节点
  char                    m_szZkDataBuf[MAX_ZK_DATA_SIZE];
  char                    m_szErr2K[2048];
  CwxMutexLock            m_mutex; ///<数据保护锁
  CWX_UINT64              m_ullVersion; ///<配置或lock的变更版本号
  string                  m_strMqZkConfig; ///<该mq实例在mq中的配置信息
  map<string, CWX_UINT8>  m_allTopicState; ///<topic/state
};

#endif
