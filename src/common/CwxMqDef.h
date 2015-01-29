#ifndef __CWX_MQ_DEF_H__
#define __CWX_MQ_DEF_H__
/**
@file CwxMqDef.h
@brief MQ系列服务的通用对象定义文件。
@author cwinux@gmail.com
@version 2.0
@date 2014-09-15
@warning
@bug
*/

#include "CwxMqMacro.h"
#include "CwxStl.h"
#include "CwxMqPoco.h"
#include "CwxDTail.h"
#include "CwxSTail.h"
#include "CwxTypePoolEx.h"
#include "CwxAppHandler4Channel.h"
#include "CwxHostInfo.h"

///topic对应的sid/timestamp信息
class CwxMqZkTopicSid {
public:
  CwxMqZkTopicSid(){
    m_ullMinSid = 0;
    m_ullSid = 0;
    m_uiTimestamp = 0;
    m_state = CWX_MQ_TOPIC_NORMAL;
  }
  CwxMqZkTopicSid(string topic, CWX_UINT64 ullMinSid, CWX_UINT64 ullSid, CWX_UINT32 uiTimestamp, CWX_UINT8 state) {
    m_strTopic = topic;
    m_state = state;
    m_ullMinSid = ullMinSid;
    m_ullSid = ullSid;
    m_uiTimestamp = uiTimestamp;
  }
  CwxMqZkTopicSid& operator=(CwxMqZkTopicSid const& item) {
    if (this != &item) {
      m_strTopic = item.m_strTopic;
      m_state = item.m_state;
      m_ullMinSid = item.m_ullMinSid;
      m_ullSid = item.m_ullSid;
      m_uiTimestamp = item.m_uiTimestamp;
    }
    return *this;
  }
public:
  string             m_strTopic; ///<对应topic
  CWX_UINT8          m_state;  ///<topic的状态：正常，冻结，删除
  CWX_UINT64         m_ullMinSid; ///该topic的最小sid
  CWX_UINT64         m_ullSid; ///<该topic最大sid
  CWX_UINT32         m_uiTimestamp; ///<该topic最大binlog的time
};

///在zk中topic下的source信息
class CwxMqZkSource{
public:
  CwxMqZkSource(){
    m_ullSid = 0;
    m_ullLastNum = 0;
  }
  CwxMqZkSource(string source, CWX_UINT64 sid, CWX_UINT64 num) {
    m_strSource = source;
    m_ullSid = sid;
    m_ullLastNum = num;
  }
  CwxMqZkSource& operator=(CwxMqZkSource const& item) {
    if (this != &item) {
      m_strSource = item.m_strSource;
      m_ullSid = item.m_ullSid;
      m_ullLastNum = item.m_ullLastNum;
    }
    return *this;
  }
public:
  string          m_strSource; ///<某topic下的source名称
  CWX_UINT64      m_ullSid; ///<source目前同步到的sid
  CWX_UINT64      m_ullLastNum; ///<剩余的binlog数目
};

///zookeeper锁的信息
class CwxMqZkLock{
public:
  CwxMqZkLock() {
    m_bMaster = false;
    m_ullPrevMasterMaxSid = 0;
    m_ullVersion = 0;
    m_unMasterInnerDispPort = 0;
    m_unPrevInnerDispPort = 0;
  }
  CwxMqZkLock(CwxMqZkLock const& item) {
    m_bMaster = item.m_bMaster;
    m_strMaster = item.m_strMaster;
    m_strMasterInnerDispHost = item.m_strMasterInnerDispHost;
    m_unMasterInnerDispPort = item.m_unMasterInnerDispPort;
    m_strPrev = item.m_strPrev;
    m_strPrevInnerDispHost = item.m_strPrevInnerDispHost;
    m_unPrevInnerDispPort = item.m_unPrevInnerDispPort;
    m_ullPrevMasterMaxSid = item.m_ullPrevMasterMaxSid;
    m_ullVersion = item.m_ullVersion;
  }
  CwxMqZkLock& operator=(CwxMqZkLock const& item) {
    if (this != &item) {
      m_bMaster = item.m_bMaster;
      m_strMaster = item.m_strMaster;
      m_strMasterInnerDispHost = item.m_strMasterInnerDispHost;
      m_unMasterInnerDispPort = item.m_unMasterInnerDispPort;
      m_strPrev = item.m_strPrev;
      m_strPrevInnerDispHost = item.m_strPrevInnerDispHost;
      m_unPrevInnerDispPort = item.m_unPrevInnerDispPort;
      m_ullPrevMasterMaxSid = item.m_ullPrevMasterMaxSid;
      m_ullVersion = item.m_ullVersion;
    }
    return *this;
  }
  bool operator==(CwxMqZkLock const& item) const {
    return (m_strMaster == item.m_strMaster)&&(m_strPrev == item.m_strPrev) ;
  }
public:
  bool                m_bMaster; ///<是否获取锁
  string              m_strMaster; ///<master的mq_id
  string              m_strMasterInnerDispHost; ///<master的内部分发host
  CWX_UINT16          m_unMasterInnerDispPort; ///<master的内部分发port
  string              m_strPrev; ///<前一个主机的mq_id
  string              m_strPrevInnerDispHost; ///<前一个主机的内部分发host
  CWX_UINT16          m_unPrevInnerDispPort; ///<前一个主机的内部分发port
  CWX_UINT64          m_ullPrevMasterMaxSid; ///前一个master的最大sid
  CWX_UINT64          m_ullVersion; ///<版本号
};

bool mqParseHostPort(string const& strHostPort, CwxHostInfo& host);

#endif
