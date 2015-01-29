#ifndef __CWX_MQ_TSS_H__
#define __CWX_MQ_TSS_H__

/**
@file CwxMqTss.h
@brief MQ系列服务的TSS定义文件。
@author cwinux@gmail.com
@version 1.0
@date 2014-09-15
@warning
@bug
*/

#include "CwxMqMacro.h"
#include "CwxLogger.h"
#include "CwxTss.h"
#include "CwxPackageReader.h"
#include "CwxPackageWriter.h"
#include "CwxBinLogMgr.h"

///定义EVENT类型
#define EVENT_ZK_CONNECTED      (CwxEventInfo::SYS_EVENT_NUM + 1) ///<建立了与zk的连接
#define EVENT_ZK_EXPIRED        (CwxEventInfo::SYS_EVENT_NUM + 2) ///<zk的连接失效
#define EVENT_ZK_FAIL_AUTH      (CwxEventInfo::SYS_EVENT_NUM + 3) ///<zk的认证失败
#define EVENT_ZK_SUCCESS_AUTH   (CwxEventInfo::SYS_EVENT_NUM + 4) ///<zk的认证成功
#define EVENT_ZK_CONF_CHANGE    (CwxEventInfo::SYS_EVENT_NUM + 5) ///<ZK的conf节点配置变化
#define EVENT_ZK_LOCK_CHANGE    (CwxEventInfo::SYS_EVENT_NUM + 6) ///<ZK的锁变化
#define EVENT_ZK_LOST_WATCH     (CwxEventInfo::SYS_EVENT_NUM + 7) ///<失去watch
#define EVENT_ZK_ERROR          (CwxEventInfo::SYS_EVENT_NUM + 8) ///<zk错误
#define EVENT_ZK_SET_SID        (CwxEventInfo::SYS_EVENT_NUM + 9) ///<设置zk的sid
#define EVENT_ZK_TOPIC_CHANGE   (CwxEventInfo::SYS_EVENT_NUM + 10) ///<zk的topic节点发生改变
#define EVENT_ZK_ADD_TOPIC      (CwxEventInfo::SYS_EVENT_NUM + 11) ///<recv通知inner/outer同步线程新增topic
#define EVENT_ZK_DEL_TOPIC      (CwxEventInfo::SYS_EVENT_NUM + 12) ///<recv通知inner/outer同步线程删除topic
#define EVENT_ZK_GROUP_EVENT     (CwxEventInfo::SYS_EVENT_NUM + 13) ///<zk中cwx-mq节点变化事件
#define EVENT_ZK_MQ_EVENT        (CwxEventInfo::SYS_EVENT_NUM + 14) ///<zk中mq节点发生变化
#define EVENT_ZK_MASTER_EVENT    (CwxEventInfo::SYS_EVENT_NUM + 15) ///<zk的master-mq节点变更


//mq的tss
class CwxMqTss : public CwxTss {
public:
  enum {
    MAX_PACKAGE_SIZE = CWX_MQ_MAX_MSG_SIZE ///<分发数据包的最大长度
  };
public:
  ///构造函数
  CwxMqTss() : CwxTss() {
    m_pReader = NULL;
    m_pItemReader = NULL;
    m_pWriter = NULL;
    m_szDataBuf = NULL;
    m_uiDataBufLen = 0;
    m_pBinlogData = NULL;
    m_pZkLock = NULL;
  }
  ///析构函数
  ~CwxMqTss();
public:
  ///tss的初始化，0：成功；-1：失败
  int init();
  ///获取package的buf，返回NULL表示失败
  inline char* getBuf(CWX_UINT32 uiSize) {
    if (m_uiDataBufLen < uiSize) {
      delete[] m_szDataBuf;
      m_szDataBuf = new char[uiSize];
      m_uiDataBufLen = uiSize;
    }
    return m_szDataBuf;
  }
  ///自己是否是master
  bool isMaster() {
    if (m_pZkLock && m_pZkLock->m_bMaster) return true;
    return false;
  }
  ///获取上一个sync的主机名
  char const* getSyncMqId() const {
    if (m_pZkLock) {
      if (m_pZkLock->m_strPrev.length()) return m_pZkLock->m_strPrev.c_str();
      if (m_pZkLock->m_strMaster.length()) return m_pZkLock->m_strMaster.c_str();
    }
    return "";
  }
  ///获取上一个sync的ip,port
  int getInnerSyncHostPort(string& host, CWX_UINT16& port) const {
    if (m_pZkLock) {
      if (m_pZkLock->m_strPrev.length()) {
        host = m_pZkLock->m_strPrevInnerDispHost;
        port = m_pZkLock->m_unPrevInnerDispPort;
        return 0;
      }
      if (m_pZkLock->m_strMaster.length()) {
        host = m_pZkLock->m_strMasterInnerDispHost;
        port = m_pZkLock->m_unMasterInnerDispPort;
        return 0;
      }
    }
    return -1;
  }
public:
  CwxMqZkLock*           m_pZkLock; ///<zk的锁信息
  CwxPackageReader*      m_pReader; ///<数据包的解包对象
  CwxPackageReader*      m_pItemReader; ///<item数据包捷报对象
  CwxPackageWriter*      m_pWriter; ///<数据包的pack对象
  CwxPackageWriter*      m_pItemWriter; ///<chunk时的一个消息的数据包的pack对象
  CwxBinLogHeader        m_header; ///<mq fetch时，发送失败消息的header
  CwxKeyValueItem        m_kvData; ///<mq fetch时，发送失败消息的数据
  CwxKeyValueItem const*  m_pBinlogData; ///<binlog的data，用于binglog的分发
  void*                 m_userData; ///<各线程的自定义数据对象指针
private:
  char*                  m_szDataBuf; ///<数据buf
  CWX_UINT32             m_uiDataBufLen; ///<数据buf的空间大小
};

#endif
