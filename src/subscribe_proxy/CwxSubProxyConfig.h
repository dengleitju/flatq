#ifndef __CWX_SUB_PROXY_CONFIG_H__
#define __CWX_SUB_PROXY_CONFIG_H__

#include "CwxMqMacro.h"
#include "CwxGlobalMacro.h"
#include "CwxHostInfo.h"
#include "CwxCommon.h"
#include "CwxIniParse.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxMqDef.h"

///mq-group节点信息
class CwxZkMqGroup {
public:
  ///构造函数
  CwxZkMqGroup() {
  }
  ///构造函数
  CwxZkMqGroup(CwxZkMqGroup const& group) {
    m_strGroup =  group.m_strGroup;
    m_strMasterId = group.m_strMasterId;
    m_masterHost = group.m_masterHost;
  }
  ///析构函数
  ~CwxZkMqGroup() {}
public:
  ///赋值操作函数
  CwxZkMqGroup& operator=(CwxZkMqGroup const& group) {
    if (this != &group) {
      m_strGroup = group.m_strGroup;
      m_strMasterId = group.m_strMasterId;
      m_masterHost = group.m_masterHost;
    }
    return *this;
  }
  ///对比函数
  bool operator==(CwxZkMqGroup const& group) {
    CWX_ASSERT(m_strGroup == group.m_strGroup);
    if (m_strMasterId != group.m_strMasterId) return false;
    if (m_masterHost.getHostName() != group.m_masterHost.getHostName()) return false;
    if (m_masterHost.getPort() != group.m_masterHost.getPort()) return false;
    return true;
  }
  ///master节点是否为空
  bool isMasterEmpty() const {
    if (m_masterHost.getHostName().length() == 0 ||
        m_masterHost.getPort() == 0) return true;
    return false;
  }
public:
  string                  m_strGroup; //<group-id
  string                  m_strMasterId; ///<master-mq的id
  CwxHostInfo             m_masterHost; ///<master-mq的host信息
};

///配置文件的common参数对象
class CwxSubConfigCmn{
public:
  CwxSubConfigCmn() {
    m_uiSockBufSize = 0;
    m_uiSyncNum = 0;
    m_uiChunkKBye = 0;
    m_bzip = true;
  }
  ~CwxSubConfigCmn() {}
public:
  string             m_strWorkDir; ///<工作目录
  CWX_UINT32         m_uiSockBufSize; ///<分发socket连接的buf大小
  CWX_UINT32         m_uiSyncNum; ///<最大发送队列数量
  CWX_UINT32         m_uiChunkKBye; ///<chunk大小
  bool               m_bzip; ///<是否sync同步
  CwxHostInfo        m_listen; ///<监听端口
};

///配置文件的zk配置信息
class CwxSubConfigZk{
public:
  CwxSubConfigZk() {}
  ~CwxSubConfigZk() {}
public:
  string          m_strZkServer; ///<zk的server地址，多个server使用;分割
  string          m_strAuth; ///<认证字符串，多个使用；分割
  string          m_strRootPath; ///<zk的根目录
};

///配置文件对象
class CwxSubConfig {
public:
  enum {
    DEF_SOCK_BUF_KB = 64,
    MIN_SOCK_BUF_KB = 4,
    MAX_SOCK_BUF_KB = 8 * 1024,
    MAX_SYNC_NUM = 10,
    MIN_SYNC_NUM = 1
  };
public:
  CwxSubConfig() {
    m_szErrMsg[0] = 0x00;
  }
  ~CwxSubConfig() {
  }
public:
  ///加载配置文件-1:失败，0：成功
  int loadConfig(string const& strConfig);
  ///输出加载的配置文件信息
  void outputConfig() const;
  ///获取common的配置信息
  inline CwxSubConfigCmn const& getCommon() const {
    return m_common;
  }
  ///获取zk的配置信息
  inline CwxSubConfigZk const& getZk() const {
    return m_zk;
  }
  ///获取错误信息
  inline char const* getErrMsg() const {
    return m_szErrMsg;
  }
private:
  bool fetchHost(CwxIniParse& cnf, string const& node, CwxHostInfo& host);
private:
  CwxSubConfigCmn      m_common; ///<common的配置信息
  CwxSubConfigZk       m_zk; ///<zk的配置信息
  char                 m_szErrMsg[204]; ///<错误消息的buf
};

#endif
