#define __STDC_FORMAT_MACROS

#include "CwxZkLocker.h"

void ZkLocker::lock_watcher_fn(zhandle_t* , int , int ,
    const char* , void *watcherCtx)
{
  //callback that we registered
  ZkLocker* locker = (ZkLocker*) watcherCtx;
  locker->lock(locker->watchType());
}

ZkLocker::ZkLocker()
{
  m_zkHandler = NULL;
  m_acl = NULL;
  m_context = NULL;
  m_completion = NULL;
  m_uiWatchType = ZK_WATCH_TYPE_PREV;
  m_lock_func = NULL;
  m_lock_func_context = NULL;
  m_bLockFail = false;
  m_uiNodeDataLen = 0;
  pthread_mutex_init(&m_mutex, NULL);
}

int ZkLocker::init(ZkAdaptor* zk,
                   string const& path,
                   string const& prex,
                   ZK_LOCK_COMPLETION completion,
                   void* context,
                   char const* data,
                   uint32_t  len,
                   ACL_vector*acl,
                   watcher_fn lock_func,
                   void* lock_func_context)
{
  m_zkHandler = zk;
  if (!path.length()){
    m_strPath = "/";
  }else{
    if (path[path.length() - 1] == '/'){
        m_strPath = path.substr(0, path.length() - 1);
    }else{
        m_strPath = path;
    }
  }
  m_strPrex = prex;
  for (uint32_t i=0; i<m_strPrex.length(); i++){
    if ('-' == m_strPrex[i]) m_strPrex[i] = '_';
  }
  m_acl = acl;
  m_context = context;
  m_completion = completion;
  m_uiWatchType = ZK_WATCH_TYPE_PREV;
  m_lock_func = lock_func;
  m_lock_func_context = lock_func_context;
  m_bLockFail = false;
  m_uiNodeDataLen = len;
  if (len > 0) {
    assert(data);
    strncpy(m_szNodeData, data, len);
  }
  return 0;
}

ZkLocker::~ZkLocker()
{
  if (m_strSelfNode.length()) unlock();
  pthread_mutex_destroy(&m_mutex);
}

///加锁；0：操作成功；-1：操作失败
int ZkLocker::lock(unsigned int uiWatchType){
  ZkMutex locker(&m_mutex);
  m_uiWatchType = uiWatchType;
  if ((m_uiWatchType != ZK_WATCH_TYPE_MASTER)&&
      (m_uiWatchType != ZK_WATCH_TYPE_PREV) &&
      (m_uiWatchType != ZK_WATCH_TYPE_ROOT))
  {
    m_uiWatchType = ZK_WATCH_TYPE_PREV;
  }
  struct Stat stat;
  int exists = m_zkHandler->nodeExists(m_strPath, stat);
  if (-1 == exists){
    m_bLockFail = true;
    return -1;
  }
  if (0 == exists){
    exists = m_zkHandler->createNode(m_strPath, NULL, 0, m_acl, 0, true);
    if (-1 == exists){
      m_bLockFail = true;
      return -1;
    }
  }
  exists = _retryLock();
  if (1 != exists){
    m_bLockFail = true;
    return -1;
  }
  return 0;
}

///释放锁；0：操作成功；-1：操作失败
int ZkLocker::unlock(){
  ZkMutex locker(&m_mutex);
  return _unlock();
}

///释放锁；0：操作成功；-1：操作失败
int ZkLocker::_unlock()
{
  if (m_strSelfNode.length()){
    string strNodePath;
    getNodePath(m_strPath, m_strSelfNode, strNodePath);
    int ret = m_zkHandler->deleteNode(strNodePath, false, -1);
    if (-1 == ret){
        return -1;
    }
    m_completion(false, m_context);
    _clearLockInfo();
  }
  return 0;
}

///0：根节点不存在；1：操作成功；-1：操作失败
int ZkLocker::_retryLock() {
  struct Stat stat;
  const clientid_t* cid = m_zkHandler->getClientId();
  /// get the session id
  uint64_t session = cid->client_id;
  int ret = 0;
  list<string> childs;
  ret = m_zkHandler->getNodeChildren(m_strPath, childs);
  if (-1 == ret) return -1;
  if (0 == ret) return 0;
  _clearLockInfo();

  uint64_t seq;
  uint64_t mySeq = 0;
  string strTmp;
  string strNodePrex;
  getNode(m_strPrex, session, strNodePrex);
  list<string>::iterator iter = childs.begin();
  while (iter != childs.end()) {
    if (splitSeqNode(*iter, seq, strTmp)) {
      if (strTmp == strNodePrex) {
        m_strSelfNode = *iter;
        getNodePath(m_strPath, m_strSelfNode, m_strSelfPathNode);
        mySeq = seq;
      }
      m_strSeqMap[seq] = *iter;
    }
    iter++;
  }
  if (!m_strSelfNode.length()) { //没有锁node
    string strNodePath;
    string strNode = strNodePrex + "-";
    getNodePath(m_strPath, strNode, strNodePath);
    ret = m_zkHandler->createNode(strNodePath,
        m_szNodeData,
        m_uiNodeDataLen,
        m_acl,
        ZOO_EPHEMERAL|ZOO_SEQUENCE,
        false,
        m_szBuf,
        MAX_SEQ_NODE_SIZE);
    if (1 != ret) {
      _clearLockInfo();
      return -1;
    }
    m_strSelfPathNode = m_szBuf;
    m_strSelfNode = getLastName(m_szBuf);
    if (splitSeqNode(m_strSelfNode, seq, strTmp)) {
      assert(strTmp == strNodePrex);
      mySeq = seq;
    }else{
      assert(0);
    }
    m_strSeqMap.clear();
    ///重新获取child
    ret = m_zkHandler->getNodeChildren(m_strPath, childs);
    if (-1 == ret) return -1;
    if (0 == ret) return 0;
    iter = childs.begin();
    while (iter != childs.end()) {
      if (splitSeqNode(*iter, seq, strTmp)) {
        m_strSeqMap[seq] = *iter;
      }
      iter++;
    }
    assert(m_strSeqMap.find(mySeq) != m_strSeqMap.end());
    assert(m_strSeqMap.find(mySeq)->second == m_strSelfNode);
  }
  ///设置owner的path
  m_strOwnerNode = m_strSeqMap.begin()->second;
  getNodePath(m_strPath, m_strOwnerNode, m_strOwnerPathNode);
  ///watch self node
  ret = m_zkHandler->wnodeExists(m_strSelfPathNode,
      stat,
      m_lock_func?m_lock_func:lock_watcher_fn,
      m_lock_func?m_lock_func_context:this);
  if (1 != ret) {
    ///解锁
    _unlock();
    return -1;
  }
  ///watch前一个或master
  map<uint64_t, string>::iterator map_iter = m_strSeqMap.find(mySeq);
  assert(map_iter != m_strSeqMap.end());
  if (map_iter == m_strSeqMap.begin()) {
    assert(m_strOwnerNode == m_strSelfNode);
    m_completion(true, m_context);
  }else{
    map_iter--;
    m_strPrevNode = map_iter->second;
    getNodePath(m_strPath, m_strPrevNode, m_strPrevPathNode);
    if (ZK_WATCH_TYPE_MASTER == m_uiWatchType) {
      ret = m_zkHandler->wnodeExists(m_strOwnerPathNode,
          stat,
          m_lock_func?m_lock_func:lock_watcher_fn,
          m_lock_func?m_lock_func_context:this);
      if (1 != ret) {
        ///解锁
        _unlock();
        return -1;
      }
    }else if (ZK_WATCH_TYPE_PREV == m_uiWatchType) {
      ret = m_zkHandler->wnodeExists(m_strPrevPathNode,
          stat,
          m_lock_func?m_lock_func:lock_watcher_fn,
          m_lock_func?m_lock_func_context:this);
      if (1 != ret) {
        ///解锁
        _unlock();
        return -1;
      }
    }else{
      list<string> childs;
      ret = m_zkHandler->wgetNodeChildren(m_strPath,
          childs,
          m_lock_func?m_lock_func:lock_watcher_fn,
          m_lock_func?m_lock_func_context:this);
      if (1 != ret) {
        ///解锁
        _unlock();
        return -1;
      }
    }
    m_completion(false, m_context);
  }
  return 1;
}

bool ZkLocker::splitNode(string const& strNode, string& strPrev, string& strSession)
{
  list<string> items;
  uint32_t num = ZkAdaptor::split(strNode, items, '-');
  if (2 != num) return false;
  list<string>::iterator iter = items.begin();
  strPrev = *iter;
  iter++;
  strSession = *iter;
  return true;
}

void ZkLocker::getNode(string const& strPrev, uint64_t clientid, string& strNode){
  char szSession[30];
  snprintf(szSession, 30, "%16.16"PRIx64, clientid);
  strNode = strPrev + "-" + szSession;
}

bool ZkLocker::splitSeqNode(string const& strSeqNode, uint64_t& seq, string& strPrev, string& strSession){
  list<string> items;
  uint32_t num = ZkAdaptor::split(strSeqNode, items, '-');
  if (3 != num) return false;
  list<string>::iterator iter = items.begin();
  strPrev = *iter;
  iter++;
  strSession = *iter;
  iter++;
  seq = strtoull(iter->c_str(), NULL, 10);
  return true;
}

bool ZkLocker::splitSeqNode(string const& strSeqNode, uint64_t& seq, string& strNode){
  list<string> items;
  uint32_t num = ZkAdaptor::split(strSeqNode, items, '-');
  if (3 != num) return false;
  list<string>::iterator iter = items.begin();
  strNode = *iter + "-";
  iter++;
  strNode += *iter;
  iter++;
  seq = strtoull(iter->c_str(), NULL, 10);
  return true;
}

///获取路径最后一个节点的名字
char const* ZkLocker::getLastName(char* str){
  char* name = strrchr(str, '/');
  if (name == NULL) return NULL;
  return (name + 1);
}
///获取节点的路径
string const& ZkLocker::getNodePath(string const& strPath, string const& strNode, string& strNodePath){
  if (strPath[strPath.length() - 1] == '/'){
      strNodePath = strPath + strNode;
  }else{
      strNodePath = strPath + "/" + strNode;
  }
  return strNodePath;
}

