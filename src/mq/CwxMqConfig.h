#ifndef __CWX_MQ_CONFIG_H__
#define __CWX_MQ_CONFIG_H__

#include "CwxMqMacro.h"
#include "CwxGlobalMacro.h"
#include "CwxHostInfo.h"
#include "CwxCommon.h"
#include "CwxIniParse.h"
#include "CwxBinLogMgr.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxMqDef.h"

CWINUX_USING_NAMESPACE

///配置文件的common参数对象
class CwxMqConfigCmn {
public:
  enum {
    MQ_TYPE_MASTER = 1,
    MQ_TYPE_SLAVE = 2,
    MQ_TYPE_ZK = 3
  };
public:
  enum {
    DEF_SOCK_BUF_KB = 64,
    MIN_SOCK_BUF_KB = 4,
    MAX_SOCK_BUF_KB = 8 * 1024,
    DEF_CHUNK_SIZE_KB = 32,
    MIN_CHUNK_SIZE_KB = 4,
    MAX_CHUNK_SIZE_KB = CWX_MQ_MAX_CHUNK_KSIZE,
    DEF_SYNC_CONN_NUM = 10,
    MIN_SYNC_CONN_NUM = 1,
    MAX_SYNC_CONN_NUM = 128
  };
public:
  CwxMqConfigCmn() {
    m_bDebug = false;
    m_uiSockBufSize = DEF_SOCK_BUF_KB;
    m_uiChunkSize = DEF_CHUNK_SIZE_KB;
    m_uiSyncConnNum = DEF_SYNC_CONN_NUM;
  }
  ;
public:
  string              m_strWorkDir; ///<工作目录
  int                 m_type; ///<mq运行类型
  CWX_UINT32          m_uiSockBufSize; ///<分发的socket连接的buf大小
  CWX_UINT32          m_uiChunkSize; ///<Trunk的大小
  CWX_UINT32          m_uiSyncConnNum;   ///<同步连接数量
  CwxHostInfo         m_monitor; ///<监控监听
  bool                m_bDebug; ///<是否打开调试
  string              m_strMqId; ///<mq的id
  string              m_strTopicConf; ///<当为master模式时，存放topic列表
};

///配置文件的binlog参数对象
class CwxMqConfigBinLog {
public:
  enum {
    DEF_BINLOG_MSIZE = 1024, ///<缺省的binlog大小
    MIN_BINLOG_MSIZE = 64, ///<最小的binlog大小
    MAX_BINLOG_MSIZE = 2048, ///<最大的binlog大小
    DEF_SAVE_BINLOG_DAY = 7, ///<缺省binlog保存天数
    MIN_SAVE_BINLOG_DAY = 0, ///<最小binlog保存天数
    MAX_SAVE_BINLOG_DAY = 14 ///<最大binlog保存天数
  };
public:
  CwxMqConfigBinLog() {
    m_uiBinLogMSize = DEF_BINLOG_MSIZE;
    m_uiMgrFileNum = CwxBinLogMgr::DEF_MANAGE_FILE_NUM;
    m_bDelOutdayLogFile = false;
    m_uiFlushNum = 100;
    m_uiFlushSecond = 30;
    m_bIsMerge = false;
    m_uiMergeSize = 256 * 1024;
    m_uiSaveFileDay = 0;
  }
public:
  string              m_strBinlogPath; ///<binlog的目录
  string              m_strBinlogPrex; ///<binlog的文件的前缀
  CWX_UINT32          m_uiBinLogMSize; ///<binlog文件的最大大小，单位为M
  CWX_UINT32          m_uiMgrFileNum; ///<管理的binglog的最大文件数
  bool                m_bDelOutdayLogFile; ///<是否删除不管理的消息文件
  CWX_UINT32          m_uiFlushNum; ///<接收多少条记录后，flush binlog文件
  CWX_UINT32          m_uiFlushSecond; ///<间隔多少秒，必须flush binlog文件
  bool                m_bIsMerge;
  CWX_UINT32          m_uiMergeSize; ///<最多merge cache大小
  CWX_UINT32          m_uiSaveFileDay; ///<binlog文件删除之后保存天数
};

///分发的参数配置对象
class CwxMqConfigDispatch {
public:
  CwxMqConfigDispatch() {
    m_uiFlushNum = 1;
    m_uiFlushSecond = 10;
  }
public:
  CwxHostInfo       m_async; ///<master bin协议异步分发端口信息
  string            m_strSourcePath; ///<source存放路径
  CWX_UINT32        m_uiFlushNum; ///<fetch多少条日志，必须flush获取点
  CWX_UINT32        m_uiFlushSecond; ///<多少秒必须flush获取点

};

///配置文件的recv参数对象
class CwxMqConfigRecv {
public:
  CwxMqConfigRecv() {
  }
public:
  CwxHostInfo     m_recv; ///<master recieve消息的listen信息
};

///配置文件的Master参数对象
class CwxMqConfigMaster {
public:
  CwxMqConfigMaster() {
    m_bzip = false;
  }
public:
  CwxHostInfo      m_master; ///<slave的master的连接信息
  bool             m_bzip; ///<是否zip压缩
};

///配置文件的zk配置信息
class CwxMqConfigZk{
public:
  CwxMqConfigZk(){}
public:
  string     m_strZkServer; ///<zk的server地址，多个server使用;分割
  string     m_strAuth; ///<认证字符串，多个使用;分割
  string     m_strRootPath; ///<zk的根路径
  string     m_strGroup; ///<mq所属group名
};
///配置文件加载对象
class CwxMqConfig {
public:
  ///构造函数
  CwxMqConfig() {
    m_szErrMsg[0] = 0x00;
  }
  ///析构函数
  ~CwxMqConfig() {
  }
public:
  //加载配置文件.-1:failure, 0:success
  int loadConfig(string const & strConfFile);
  //输出加载的配置文件信息
  void outputConfig() const;
  ///slave模式时更新topic-state信息
  int saveTopic(string const& strTopicFile, map<string, CWX_UINT8>& topics);
  ///加载topic配置信息
  int  loadTopic(string const& strTopicFile);
public:
  ///获取common配置信息
  inline CwxMqConfigCmn const& getCommon() const {
    return m_common;
  }
  ///获取binlog配置信息
  inline CwxMqConfigBinLog const& getBinLog() const {
    return m_binlog;
  }
  ///获取master配置信息
  inline CwxMqConfigMaster const& getMaster() const {
    return m_master;
  }
  ///获取内部分发的信息
  inline CwxMqConfigDispatch const& getInnerDispatch() const {
    return m_innerDispatch;
  }
  //获取外部分发信息
  inline CwxMqConfigDispatch const& getOuterDispatch() const {
    return m_outerDispatch;
  }
  ///获取消息接受的listen信息
  inline CwxMqConfigRecv const& getRecv() const {
    return m_recv;
  }
  ///获取zk的配置信息
  inline CwxMqConfigZk const& getZk() const {
    return m_zk;
  }
  ///当master类型时，获取topic列表
  inline map<string, CWX_UINT8> getTopics() const {
    return m_topics;
  }
  ///获取配置文件加载的失败原因
  inline char const* getErrMsg() const {
    return m_szErrMsg;
  }
  ;
private:
  bool fetchHost(CwxIniParse& cnf, string const& node, CwxHostInfo& host);
private:
  CwxMqConfigCmn           m_common; ///<common的配置信息
  CwxMqConfigBinLog        m_binlog; ///<binlog的配置信息
  CwxMqConfigMaster        m_master; ///<slave的master的数据同步配置信息
  CwxMqConfigRecv          m_recv; ///<master的数据接收listen信息
  CwxMqConfigDispatch      m_innerDispatch; ///<内部dispatch的配置信息
  CwxMqConfigDispatch      m_outerDispatch; ///<外部dispatch的配置信息
  CwxMqConfigZk            m_zk; ///<zk的配置信息
  map<string, CWX_UINT8>   m_topics; ///<管辖的topic列表
  char                     m_szErrMsg[2048]; ///<错误消息的buf
};

#endif
