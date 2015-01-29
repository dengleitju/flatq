#ifndef __ZK_LOCKER_H__
#define __ZK_LOCKER_H__
#include "CwxZkAdaptor.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <limits.h>
#include <stdbool.h>
#include <assert.h>

class ZkMutex{
public:
  ZkMutex(pthread_mutex_t* mutex) {
    m_mutex = mutex;
    if (m_mutex) pthread_mutex_lock(m_mutex);
  }
  ~ZkMutex() {
    if (m_mutex) pthread_mutex_unlock(m_mutex);
  }
private:
  pthread_mutex_t*    m_mutex;
};

typedef void (* ZK_LOCK_COMPLETION) (bool bLock, void* context);

class ZkLocker{
public:
  enum{
    MAX_SEQ_NODE_SIZE = 2048
  };
  enum{
    ZK_WATCH_TYPE_MASTER=1,
    ZK_WATCH_TYPE_PREV=2,
    ZK_WATCH_TYPE_ROOT=3
  };
public:
  ZkLocker();
  ~ZkLocker();
public:
  /// 初始化
  int init(ZkAdaptor* zk,
      string const& path,
      string const& prex,
      ZK_LOCK_COMPLETION completion,
      void* context=NULL,
      char const* data=NULL,
      uint32_t len=0,
      ACL_vector* acl = &ZOO_OPEN_ACL_UNSAFE,
      watcher_fn lock_func=NULL,
      void* lock_func_context=NULL);
  /// 加锁：0：成功 ；-1：失败
  int lock(unsigned int uiWatchType=ZK_WATCH_TYPE_PREV);
  /// 释放锁；0：成功；-1：失败
  int unlock();
public:
  /// 是否watch master
  inline unsigned int watchType() const{
    return m_uiWatchType;
  }
  /// 是否加锁失败
  inline bool isLockFail() {
    ZkMutex locker(&m_mutex);
    return m_bLockFail;
  }
  /// 获取所有的locker
  inline void getLocker(list<string>& lockers) {
    ZkMutex locker(&m_mutex);
    _getLocker(lockers);
  }
  /// 是否加锁
  bool isLocked() {
    ZkMutex locker(&m_mutex);
    return _isLocked();
  }
  /// 获取自己的锁node
  void getSelfNode(string& strNode) {
    ZkMutex locker(&m_mutex);
    _getSelfNode(strNode);
  }
  /// 获取自己的锁node的path name
  void getSelfPathNode(string& strPathNode) {
    ZkMutex locker(&m_mutex);
    _getSelfPathNode(strPathNode);
  }
  /// 获取owner的锁node
  void getOwnerNode(string& strNode) {
    ZkMutex locker(&m_mutex);
    _getOwnerNode(strNode);
  }
  /// 获取owner锁的path name
  void getOwnerPathNode(string& strPathNode) {
    ZkMutex locker(&m_mutex);
    _getOwnerPathNode(strPathNode);
  }
  ///获取prev的锁node
  void getPrevNode(string& strNode){
    ZkMutex locker(&m_mutex);
    _getPrevNode(strNode);
  }
  ///获取Prev的锁node的path name
  void getPrevPathNode(string& strPathNode){
    ZkMutex locker(&m_mutex);
    _getPrevPathNode(strPathNode);
  }
  ///获取所有的locker
  inline void _getLocker(list<string>& locers){
    locers.clear();
    map<uint64_t, string>::iterator iter = m_strSeqMap.begin();
    while(iter != m_strSeqMap.end()){
      locers.push_back(iter->second);
      iter++;
    }
    return;
  }
  ///是否加锁
  bool _isLocked(){
    if (m_strOwnerNode.length()){
        return m_strOwnerNode == m_strSelfNode;
    }
    return false;
  }
  ///获取自己的锁node
  void _getSelfNode(string& strNode){
      strNode = m_strSelfNode;
  }
  ///获取自己的锁node的path name
  void _getSelfPathNode(string& strPathNode){
      strPathNode = m_strSelfPathNode;
  }
  ///获取owner的锁node
  void _getOwnerNode(string& strNode){
      strNode = m_strOwnerNode;
  }
  ///获取owner的锁node的path name
  void _getOwnerPathNode(string& strPathNode){
      strPathNode = m_strOwnerPathNode;
  }
  ///获取prev的锁node
  void _getPrevNode(string& strNode){
      strNode = m_strPrevNode;
  }
  ///获取Prev的锁node的path name
  void _getPrevPathNode(string& strPathNode){
      strPathNode = m_strPrevPathNode;
  }
public:
  static void lock_watcher_fn(zhandle_t* , int , int ,
      const char* , void *watcherCtx);
  static bool splitSeqNode(string const& strSeqNode, uint64_t& seq, string& strNode);
  ///对获取加锁者各部分信息
  static bool splitSeqNode(string const& strSeqNode, uint64_t& seq, string& strPrev, string& strSession);
  ///对获取加锁者各部分信息
  static bool splitNode(string const& strNode, string& strPrev, string& strSession);
  ///获取node的
  static void getNode(string const& strPrev, uint64_t clientid, string& strNode);
  ///获取路径最后一个节点的名字
  static char const* getLastName(char* str);
  ///获取节点的路径
  static string const& getNodePath(string const& strPath, string const& strNode, string& strNodePath);
private:
  ///加锁；0：根节点不存在；1：操作成功；-1：操作失败
  int _retryLock();
  ///释放锁；0：操作成功；-1：操作失败
  int _unlock();

  inline void _clearLockInfo(){
    m_strSelfNode = "";
    m_strSelfPathNode = "";
    m_strOwnerNode = "";
    m_strOwnerPathNode = "";
    m_strPrevNode = "";
    m_strPrevPathNode = "";
    m_strSeqMap.clear();
    m_bLockFail = false;
  }
private:
  ZkAdaptor*           m_zkHandler; ///zk handler
  string               m_strPath; ///锁路径
  string               m_strPrex; ///锁前缀，node为[prex]-[session-id]-[seq]格式
  struct ACL_vector*   m_acl; ///锁权限
  void *               m_context;  ///<环境信息
  ZK_LOCK_COMPLETION   m_completion; ///锁变化通知函数
  pthread_mutex_t      m_mutex; ///锁对象
  string               m_strSelfNode; ///自己的节点名字,相对路径(不包含strPath)
  string               m_strSelfPathNode; ///节点的全路径path
  string               m_strOwnerNode; ///锁拥有者的节点名字
  string               m_strOwnerPathNode; ///锁拥有者的全路径path
  string               m_strPrevNode; ///自己前一个节点的名字
  string               m_strPrevPathNode; ///自己前一个节点的全路径path
  char                 m_szBuf[MAX_SEQ_NODE_SIZE]; ///seq节点的值
  char                 m_szNodeData[MAX_SEQ_NODE_SIZE]; ///设置节点的data数据
  uint32_t             m_uiNodeDataLen; ///设置节点data数据长度
  map<uint64_t, string> m_strSeqMap; ///各个session的seq的map
  unsigned int         m_uiWatchType; ///是否watch master. 缺省watch前一个防止惊群
  watcher_fn           m_lock_func; ///锁变化的回调function
  void*                m_lock_func_context; ///锁变化的回调context
  bool                 m_bLockFail; ///是否加锁失败
};

#endif
