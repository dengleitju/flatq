#ifndef __CWX_PRODUCT_CONFIG_H__
#define __CWX_PRODUCT_CONFIG_H__

#include "CwxMqMacro.h"
#include "CwxGlobalMacro.h"
#include "CwxHostInfo.h"
#include "CwxCommon.h"
#include "CwxIniParse.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxMqDef.h"


///mq-group节点信息
class CwxMqGroup {
public:
  ///构造函数
  CwxMqGroup() {
    m_unMasterPort = 0;
    m_bWatcherTopic = false;
  }
  ///构造函数
  CwxMqGroup(CwxMqGroup const& group) {
    m_strGroup =  group.m_strGroup;
    m_strMasterId = group.m_strMasterId;
    m_strMasterHost = group.m_strMasterHost;
    m_unMasterPort = group.m_unMasterPort;
    m_bWatcherTopic = group.m_bWatcherTopic;
  }
  ///析构函数
  ~CwxMqGroup() {}
public:
  ///赋值操作函数
  CwxMqGroup& operator=(CwxMqGroup const& group) {
    if (this != &group) {
      m_strGroup = group.m_strGroup;
      m_strMasterId = group.m_strMasterId;
      m_strMasterHost = group.m_strMasterHost;
      m_unMasterPort = group.m_unMasterPort;
      m_bWatcherTopic = group.m_bWatcherTopic;
    }
    return *this;
  }
  ///对比函数
  bool operator==(CwxMqGroup const& group) {
    CWX_ASSERT(m_strGroup == group.m_strGroup);
    if (m_strMasterId != group.m_strMasterHost) return false;
    if (m_strMasterHost != group.m_strMasterHost) return false;
    if (m_unMasterPort != group.m_unMasterPort) return false;
    return true;
  }
  ///master节点是否为空
  bool isMasterEmpty() const {
    if (m_strMasterHost.length() == 0 || !m_unMasterPort) return true;
    return false;
  }
  ///是否watcher了topic
  bool isWathTopic() const {
    return m_bWatcherTopic;
  }
  ///设置watcher
  void setWathTopic()  {
    m_bWatcherTopic = true;
  }
  ///清空watcher
  void clearWathTopic()  {
    m_bWatcherTopic = false;
  }
public:
  string                  m_strGroup; //<group-id
  string                  m_strMasterId; ///<master-mq的id
  string                  m_strMasterHost; ///<master-mq的host
  CWX_UINT16              m_unMasterPort; ///<master-mq的port
  bool                    m_bWatcherTopic; ///是否watcher topic
};


///cwx-mq连接对象
class CwxMqGroupConn{
public:
  CwxMqGroupConn() {
    m_uiMqConnId = -1;
  }
  CwxMqGroupConn(CwxMqGroup const& group, CWX_INT32 conn_id) : m_zkGroup(group) {
    m_uiMqConnId = conn_id;
  }
  ~CwxMqGroupConn() {}
public:
  CwxMqGroup       m_zkGroup;
  CWX_INT32        m_uiMqConnId;
};

///zk中topic-group对象
class CwxMqTopicGroup {
public:
  CwxMqTopicGroup(string strTopic) {
    m_strTopic = strTopic;
  }
  CwxMqTopicGroup(CwxMqTopicGroup const& topic) {
    m_strTopic = topic.m_strTopic;
    m_strPrevGroup = topic.m_strPrevGroup;
    m_groups.clear();
    list<string>::const_iterator iter = topic.m_groups.begin();
    while (iter != topic.m_groups.end()) {
      m_groups.push_back(*iter);
      iter++;
    }
  }
  ~CwxMqTopicGroup() {}
public:
  CwxMqTopicGroup& operator=(CwxMqTopicGroup const& topic) {
    if (&topic == this) return *this;
    m_strTopic = topic.m_strTopic;
    m_strPrevGroup = topic.m_strPrevGroup;
    m_groups.clear();
    list<string>::const_iterator iter = topic.m_groups.begin();
    while (iter != topic.m_groups.end()) {
      m_groups.push_back(*iter);
      iter++;
    }
    return *this;
  }
  ///删除group,返回group数量
  int eraseGroup(string strGroup) {
    list<string>::iterator iter = find(m_groups.begin(), m_groups.end(), strGroup);
    if (iter != m_groups.end()) {
      m_groups.erase(iter);
    }
    return m_groups.size();
  }
  ///添加group,返回group数量
  int addGroup(string strGroup) {
    list<string>::iterator iter = find(m_groups.begin(), m_groups.end(), strGroup);
    if (iter == m_groups.end()) {
      m_groups.push_back(strGroup);
    }
    return m_groups.size();
  }
  ///获取下一个group
  string getNextGroup() {
    list<string>::iterator iter = find(m_groups.begin(), m_groups.end(), m_strPrevGroup);
    if (iter == m_groups.end() || (++iter == m_groups.end())) {
      m_strPrevGroup = m_groups.front();
    }else {
      m_strPrevGroup = *iter;
    }
    return m_strPrevGroup;
  }
public:
  list<string>     m_groups; ///<对应group
  string           m_strTopic;
  string           m_strPrevGroup; ///<上次发送的group
};


///配置文件的common参数对象
class CwxProductConfigCmn{
public:
  enum{
    MAX_QUEUE_NUM = 100000,
    MIN_QUEUE_NUM = 30
  };
public:
  CwxProductConfigCmn() {
    m_uiSockBufSize = 0;
    m_uiSendTimeout = 0;
    m_uiMaxQueueNum = 10;
  }
  ~CwxProductConfigCmn() {}
public:
  string             m_strWorkDir; ///<工作目录
  CWX_UINT32         m_uiSockBufSize; ///<分发socket连接的buf大小
  CWX_UINT32         m_uiSendTimeout; ///<发送多久没有返回算超时单位秒
  CWX_UINT32         m_uiMaxQueueNum; ///<最大发送队列数量
  CwxHostInfo        m_listen; ///<监听端口
};

///配置文件的zk配置信息
class CwxProductConfigZk{
public:
  CwxProductConfigZk() {}
  ~CwxProductConfigZk() {}
public:
  string          m_strZkServer; ///<zk的server地址，多个server使用;分割
  string          m_strAuth; ///<认证字符串，多个使用；分割
  string          m_strRootPath; ///<zk的根目录
};

///配置文件对象
class CwxProductConfig {
public:
  enum {
    DEF_SOCK_BUF_KB = 64,
    MIN_SOCK_BUF_KB = 4,
    MAX_SOCK_BUF_KB = 8 * 1024,
    DEF_SEND_TIMEOUT = 3,
    MIN_SEND_TIMEOUT = 1,
    MAX_SEND_TIMEOUT = 10
  };
public:
  CwxProductConfig() {
    m_szErrMsg[0] = 0x00;
  }
  ~CwxProductConfig() {
  }
public:
  ///加载配置文件-1:失败，0：成功
  int loadConfig(string const& strConfig);
  ///输出加载的配置文件信息
  void outputConfig() const;
  ///获取common的配置信息
  inline CwxProductConfigCmn const& getCmmon() const {
    return m_common;
  }
  ///获取zk的配置信息
  inline CwxProductConfigZk const& getZk() const {
    return m_zk;
  }
  ///获取错误信息
  inline char const* getErrMsg() const {
    return m_szErrMsg;
  }
private:
  bool fetchHost(CwxIniParse& cnf, string const& node, CwxHostInfo& host);
private:
  CwxProductConfigCmn      m_common; ///<common的配置信息
  CwxProductConfigZk       m_zk; ///<zk的配置信息
  char                     m_szErrMsg[204]; ///<错误消息的buf
};

#endif
