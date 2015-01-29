#define __STDC_FORMAT_MACROS
#include "CwxZkAdaptor.h"
#include <string.h>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <sys/select.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <inttypes.h>
#include <stddef.h>

/// adaptor内部认证回调函数
void ZkAdaptor::authCompletion(int rc, const void *data) {
  ZkAdaptor* zk = (ZkAdaptor*)data;
  if (ZOK == rc) {
    zk->m_iAuthState = AUTH_STATE_SUCCESS;
  }else{
    zk->m_iAuthState = AUTH_STATE_FAIL;
  }
}

ZkAdaptor::ZkAdaptor(string const& strHost, uint32_t uiRecvTimeout)
{
  m_strHost = strHost;
  m_uiRecvTimeout = uiRecvTimeout;
  m_iAuthState = AUTH_STATE_SUCCESS;
  m_zkHandle = NULL;
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
}

ZkAdaptor::~ZkAdaptor()
{
  disconnect();
}

int ZkAdaptor::init(ZooLogLevel level)
{
  zoo_set_debug_level(level);
  return 0;
}

/// 内部默认watcher函数
void ZkAdaptor::watcher(zhandle_t *t, int type, int state, const char *path,
    void* context)
{
  ZkAdaptor* adapter=(ZkAdaptor*)context;
  adapter->onEvent(t, type, state, path);
}

/// 事件回调函数，所有事件的根api
void ZkAdaptor::onEvent(zhandle_t *, int type, int state, const char *path)
{
  if (type == ZOO_SESSION_EVENT) {
    if (state == ZOO_CONNECTED_STATE){
      return onConnect();
    } else if (state == ZOO_AUTH_FAILED_STATE) {
      return onFailAuth();
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      return onExpired();
    } else if (state == ZOO_CONNECTING_STATE){
      return onConnecting();
    } else if (state == ZOO_ASSOCIATING_STATE){
      return onAssociating();
    }
  }else if (type == ZOO_CREATED_EVENT){
    return onNodeCreated(state, path);
  }else if (type == ZOO_DELETED_EVENT){
    return onNodeDeleted(state, path);
  }else if (type == ZOO_CHANGED_EVENT){
    return onNodeChanged(state, path);
  }else if (type == ZOO_CHILD_EVENT){
    return onNodeChildChanged(state, path);
  }else if (type == ZOO_NOTWATCHING_EVENT){
    return onNoWatching(state, path);
  }
  onOtherEvent(type, state, path);
}

int ZkAdaptor::connect(const clientid_t *clientid, int flags, watcher_fn watch, void *context)
{
  // Clear the connection state
  disconnect();
  m_iAuthState = AUTH_STATE_SUCCESS;
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
  // Establish a new connection to ZooKeeper
  m_zkHandle = zookeeper_init(m_strHost.c_str(),
          watch?watch:ZkAdaptor::watcher,
          m_uiRecvTimeout,
          clientid,
          watch?context:this,
          flags);

  if (m_zkHandle == NULL) {
    snprintf(m_szErr2K, 2047, "Unable to connect to ZK running at '%s'", m_strHost.c_str());
    return -1;
  }
  return 0;
}

void ZkAdaptor::disconnect()
{
  if (m_zkHandle != NULL)
  {
    zookeeper_close (m_zkHandle);
    m_zkHandle = NULL;
  }
}

bool ZkAdaptor::addAuth(const char* scheme, const char* cert, int certLen, uint32_t timeout, void_completion_t completion, const void *data)
{
  int rc;
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return false;
  }

  m_iAuthState = AUTH_STATE_WAITING;
  rc = zoo_add_auth(m_zkHandle, scheme, cert, certLen, completion?completion:ZkAdaptor::authCompletion, completion?data:this);
  if (rc != ZOK) // check return status
  {
    m_iErrCode = rc;
    m_iAuthState = AUTH_STATE_FAIL;
    snprintf(m_szErr2K, 2047, "Error in auth , err:%s, err-code:%d.", zerror(rc), rc);
    return false;
  }
  int delay=5;
  while (timeout)
  {
    if (timeout < 5)
    {
      delay = timeout;
    }
    ZkAdaptor::sleep(delay);
    timeout -= delay;
    if (AUTH_STATE_WAITING == getAuthState())
    {
      continue;
    }
    else if (AUTH_STATE_FAIL == getAuthState())
    {
      strcpy(m_szErr2K, "failure to auth.");
      return false;
    }
    return true;
  }
  strcpy(m_szErr2K, "add auth timeout.");
  return false;
}


int ZkAdaptor::createNode(const string &path,
                           char const* data,
                           uint32_t dataLen,
                           const struct ACL_vector *acl,
                           int flags,
                           bool recursive,
                           char* pathBuf,
                           uint32_t pathBufLen)
{
    const int MAX_PATH_LENGTH = 2048;
    char realPath[MAX_PATH_LENGTH];
    realPath[0] = 0;
    int rc;
    m_iErrCode = 0;
    memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

    if (!isConnected())
    {
      strcpy(m_szErr2K, "No connect");
      return -1;
    }

    if (!pathBuf)
    {
      pathBuf = realPath;
      pathBufLen = MAX_PATH_LENGTH;
    }
    rc = zoo_create( m_zkHandle,
        path.c_str(),
        data,
        dataLen,
        acl?acl:&ZOO_OPEN_ACL_UNSAFE,
        flags,
        pathBuf,
        pathBufLen);
    if (recursive && (ZNONODE == rc)){
      for (string::size_type pos = 1; pos != string::npos; ){
        pos = path.find( "/", pos );
        if (pos != string::npos){
          if (-1 == (rc = createNode(path.substr( 0, pos ), NULL, 0, acl, 0, true, pathBuf, pathBufLen))){
            return -1;
          }
          pos++;
        }else{
          // No more path components
          return createNode(path, data, dataLen, acl, 0, false, pathBuf, pathBufLen);
        }
      }
    }
    if (rc != ZOK) // check return status
    {
        m_iErrCode = rc;
        if (ZNODEEXISTS == rc) return 0;
        snprintf(m_szErr2K, 2047, "Error in creating ZK node [%s], err:%s err-code:%d.", path.c_str(), zerror(rc), rc);
        return -1;
    }
    return 1;
}

int ZkAdaptor::deleteNode(const string &path,
                           bool recursive,
                           int version)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }

  int rc;
  rc = zoo_delete( m_zkHandle, path.c_str(), version);

  if (rc != ZOK) //check return status
  {
    m_iErrCode = rc;
    if (ZNONODE == rc) return 0;
    if ((rc == ZNOTEMPTY) && recursive)
    {
      list<string> childs;
      if (!getNodeChildren(path, childs)) return false;
      string strPath;
      list<string>::iterator iter=childs.begin();
      while(iter != childs.end())
      {
        strPath = path + "/" + *iter;
        if (!deleteNode(strPath, true)) return false;
        iter++;
      }
      return deleteNode(path);
    }
    snprintf(m_szErr2K, 2047, "Unable to delete zk node [%s], err:%s  err-code=%d", path.c_str(), zerror(rc), rc);
    return -1;
  }
  return 1;
}

int ZkAdaptor::getNodeChildren( const string &path, list<string>& childs, int watch)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }

  String_vector children;
  memset( &children, 0, sizeof(children) );
  int rc;
  rc = zoo_get_children( m_zkHandle,
    path.c_str(),
    watch,
    &children );

  if (rc != ZOK) // check return code
  {
    m_iErrCode = rc;
    if (rc == ZNONODE) return 0;
    snprintf(m_szErr2K, 2047, "Failure to get node [%s] child, err:%s err-code:%d", path.c_str(), zerror(rc), rc);
    return -1;
  }
  childs.clear();
  for (int i = 0; i < children.count; ++i)
  {
    childs.push_back(string(children.data[i]));
  }
  //释放
  if (children.data) {
    int32_t i;
    for (i=0; i<children.count; i++) {
        free(children.data[i]);
    }
    free(children.data);
  }
  return 1;
}

int ZkAdaptor::nodeExists(const string &path, struct Stat& stat, int watch)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }

  memset(&stat, 0, sizeof(stat) );
  int rc;
  rc = zoo_exists( m_zkHandle,
      path.c_str(),
      watch,
      &stat);
  if (rc != ZOK)
  {
    if (rc == ZNONODE) return 0;
    m_iErrCode = rc;
    snprintf(m_szErr2K, 2047, "Error in checking existance of [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
    return -1;
  }
  return 1;
}

int ZkAdaptor::getNodeData(const string &path, char* data, uint32_t& dataLen, struct Stat& stat, int watch)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }

  memset(&stat, 0, sizeof(stat) );

  int rc = 0;
  int len = dataLen;
  rc = zoo_get( m_zkHandle,
      path.c_str(),
      watch,
      data,
      &len,
      &stat);
  if (rc != ZOK) // checl return code
  {
    m_iErrCode = rc;
    if (rc == ZNONODE) return 0;
    snprintf(m_szErr2K, 2047, "Error in fetching value of [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
    return -1;
  }
  dataLen = len;
  data[len] = 0x00;
  return 1;
}

int ZkAdaptor::wgetNodeChildren( const string &path, list<string>& childs, watcher_fn watcher, void* watcherCtx)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }

  String_vector children;
  memset( &children, 0, sizeof(children) );
  int rc;
  rc = zoo_wget_children( m_zkHandle,
    path.c_str(),
    watcher,
    watcherCtx,
    &children );

  if (rc != ZOK) // check return code
  {
    m_iErrCode = rc;
    if (rc == ZNONODE) return 0;
    snprintf(m_szErr2K, 2047, "Failure to get node [%s] child, err:%s err-code:%d", path.c_str(), zerror(rc), rc);
    return -1;
  }
  childs.clear();
  for (int i = 0; i < children.count; ++i)
  {
    childs.push_back(string(children.data[i]));
  }
  return 1;
}

int ZkAdaptor::wnodeExists(const string &path, struct Stat& stat, watcher_fn watcher, void* watcherCtx)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }

  memset(&stat, 0, sizeof(stat) );
  int rc;
  rc = zoo_wexists( m_zkHandle,
      path.c_str(),
      watcher,
      watcherCtx,
      &stat);
  if (rc != ZOK)
  {
    if (rc == ZNONODE) return 0;
    m_iErrCode = rc;
    snprintf(m_szErr2K, 2047, "Error in checking existance of [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
    return -1;
  }
  return 1;
}

int ZkAdaptor::wgetNodeData(const string &path, char* data, uint32_t& dataLen, struct Stat& stat, watcher_fn watcher, void* watcherCtx)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }

  memset(&stat, 0, sizeof(stat) );

  int rc = 0;
  int len = dataLen;
  rc = zoo_wget( m_zkHandle,
      path.c_str(),
      watcher,
      watcherCtx,
      data,
      &len,
      &stat);
  if (rc != ZOK) // checl return code
  {
    m_iErrCode = rc;
    if (rc == ZNONODE) return 0;
    snprintf(m_szErr2K, 2047, "Error in fetching value of [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
    return -1;
  }
  dataLen = len;
  data[len] = 0x00;
  return 1;
}

int ZkAdaptor::setNodeData(const string &path, char const* data, uint32_t dataLen, int version)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));
  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }
  int rc;
  rc = zoo_set( m_zkHandle,
      path.c_str(),
      data,
      dataLen,
      version);
  if (rc != ZOK) // check return code
  {
    m_iErrCode = rc;
    if (rc == ZNONODE) return 0;
    snprintf(m_szErr2K, 2047, "Error in set value of [%s], err:%s err-code:%d", path.c_str(), zerror(rc), rc);
    return -1;
  }
  // success
  return 1;
}

int ZkAdaptor::getAcl(const char *path, struct ACL_vector& acl, struct Stat& stat)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
    strcpy(m_szErr2K, "No connect");
    return -1;
  }

  int rc;
  memset(&stat, 0x00, sizeof(stat));
  rc = zoo_get_acl( m_zkHandle,
      path,
      &acl,
      &stat);

  if (rc != ZOK) // check return code
  {
    m_iErrCode = rc;
    if (rc == ZNONODE) return 0;

    snprintf(m_szErr2K, 2047, "Error in get acl for [%s], err:%s err-code:%d", path, zerror(rc), rc);
    return -1;
  }
  // success
  return 1;
}

int ZkAdaptor::setAcl(const char *path, const struct ACL_vector *acl, bool recursive, int version)
{
  m_iErrCode = 0;
  memset(m_szErr2K, 0x00, sizeof(m_szErr2K));

  if (!isConnected())
  {
      strcpy(m_szErr2K, "No connect");
      return -1;
  }

  int rc;
  rc = zoo_set_acl( m_zkHandle,
      path,
      version,
      acl?acl:&ZOO_OPEN_ACL_UNSAFE);

  if (rc != ZOK) // check return code
  {
    m_iErrCode = rc;
    if (rc == ZNONODE) return 0;
    snprintf(m_szErr2K, 2047, "Error in set acl for [%s], err:%s err-code:%d", path, zerror(rc), rc);
    return -1;
  }
  if (recursive){
    list<string> childs;
    list<string>::iterator iter;
    string local_path;
    if (1 == getNodeChildren(path, childs, 0)){
      iter = childs.begin();
      while(iter != childs.end()){
        local_path = path;
        local_path +="/";
        local_path += *iter;
        ZkAdaptor::setAcl(local_path.c_str(), acl, true, version);
        iter++;
      }
    }
  }
  // success
  return 1;
}

void ZkAdaptor::sleep(uint32_t miliSecond)
{
  struct timeval tv;
  tv.tv_sec = miliSecond/1000;
  tv.tv_usec = (miliSecond%1000)*1000;
  select(1, NULL, NULL, NULL, &tv);
}

char* ZkAdaptor::base64(const unsigned char *input, int length)
{
  BIO *bmem, *b64;
  BUF_MEM *bptr;

  b64 = BIO_new(BIO_f_base64());
  bmem = BIO_new(BIO_s_mem());
  b64 = BIO_push(b64, bmem);
  BIO_write(b64, input, length);
  BIO_flush(b64);
  BIO_get_mem_ptr(b64, &bptr);

  char *buff = (char *)malloc(bptr->length);
  memcpy(buff, bptr->data, bptr->length-1);
  buff[bptr->length-1] = 0;
  BIO_free_all(b64);
  return buff;
}

void ZkAdaptor::sha1(char const* input, int length, unsigned char *output)
{
  SHA_CTX   c;
  SHA1_Init(&c);
  SHA1_Update(&c, input, length);
  SHA1_Final(output, &c);
}

char* ZkAdaptor::digest(char const* input, int length)
{
  unsigned char output[20];
  sha1(input, length, output);
  return base64(output, 20);
}

int ZkAdaptor::split(string const& src, list<string>& value, char ch)
{
  string::size_type begin = 0;
  string::size_type end = src.find(ch);
  int num=1;
  value.clear();
  while(string::npos != end)
  {
    value.push_back(src.substr(begin, end - begin));
    begin = end + 1;
    end = src.find(ch, begin);
    num++;
  }
  value.push_back(src.substr(begin));
  return num;
}

//可以为all,self,read或者user:passwd:acrwd
bool ZkAdaptor::fillAcl(char const* priv, struct ACL& acl)
{
  struct ACL _OPEN_ACL_UNSAFE_ACL[] = {{0x1f, {"world", "anyone"}}};
  struct ACL _READ_ACL_UNSAFE_ACL[] = {{0x01, {"world", "anyone"}}};
  struct ACL _CREATOR_ALL_ACL_ACL[] = {{0x1f, {"auth", ""}}};

  if (!priv) return false;
  if (strcmp("all", priv) == 0){
      memcpy(&acl, &_OPEN_ACL_UNSAFE_ACL[0], sizeof(acl));
      return true;
  }else if (strcmp("self", priv) == 0){
      memcpy(&acl, &_CREATOR_ALL_ACL_ACL[0], sizeof(acl));
      return true;
  }else if (strcmp("read", priv) == 0){
      memcpy(&acl, &_READ_ACL_UNSAFE_ACL[0], sizeof(acl));
      return true;
  }
  list<string> items;
  string scheme;
  string user;
  string passwd;
  string perms;
  string ip;
  int i=0;
  split(string(priv), items, ':');
  list<string>::iterator iter = items.begin();

  scheme = *iter; iter++;

  if (scheme == "digest"){
    if (4 != items.size()) return false;
    user = *iter; iter++;
    passwd = *iter; iter++;
    perms = *iter;
    string strId = user + ":" + passwd;
    char* id = digest(strId.c_str(), strId.length());
    user = user + ":" + id;
    acl.id.scheme = "digest";
    acl.id.id = strdup(user.c_str());
    free(id);
  }else if (scheme == "ip"){
    if (3 != items.size()) return false;
    ip = *iter; iter++;
    perms = *iter;
    acl.id.scheme = "ip";
    acl.id.id = strdup(ip.c_str());
  }else{
    return false;
  }
  acl.perms = 0;
  while(perms[i])
  {
    switch(perms[i])
    {
    case 'a':
      acl.perms|=ZOO_PERM_ADMIN;
      break;
    case 'r':
      acl.perms|=ZOO_PERM_READ;
      break;
    case 'w':
      acl.perms|=ZOO_PERM_WRITE;
      break;
    case 'c':
      acl.perms|=ZOO_PERM_CREATE;
      break;
    case 'd':
      acl.perms|=ZOO_PERM_DELETE;
      break;
    default:
      return false;
    }
    i++;
  }
  return true;
}

///输出权限信息，每行一个权限
void ZkAdaptor::dumpAcl(ACL_vector const& acl, list<string>& info)
{
  char line[1024];
  info.clear();
  for (int i=0; i<acl.count; i++)
  {
    snprintf(line, 1024, "%s%s%s%s%s:%s:%s",
        (acl.data[i].perms&ZOO_PERM_READ)==ZOO_PERM_READ?"r":"",
        (acl.data[i].perms&ZOO_PERM_WRITE)==ZOO_PERM_WRITE?"w":"",
        (acl.data[i].perms&ZOO_PERM_CREATE)==ZOO_PERM_CREATE?"c":"",
        (acl.data[i].perms&ZOO_PERM_DELETE)==ZOO_PERM_DELETE?"d":"",
        (acl.data[i].perms&ZOO_PERM_ADMIN)==ZOO_PERM_READ?"a":"",
        acl.data[i].id.scheme?acl.data[i].id.scheme:"",
        acl.data[i].id.id?acl.data[i].id.id:"");
    info.push_back(string(line));
  }
}

static char const* toString(int64_t llNum, char* szBuf, int base)
{
  char const* szFormat=(16==base)?"%"PRIx64:"%"PRId64;
  sprintf(szBuf, szFormat, llNum);
  return szBuf;
}

///输出节点的信息,一行一个信息项
void ZkAdaptor::dumpStat(struct Stat const& stat, string& info)
{
  char szTmp[64];
  char line[1024];
  time_t timestamp;
  snprintf(line, 1024, "czxid:%s\n", toString(stat.czxid, szTmp, 16));
  info = line;

  snprintf(line, 1024, "mzxid:%s\n", toString(stat.mzxid, szTmp, 16));
  info += line;

  timestamp = stat.ctime/1000;
  snprintf(line, 1024, "ctime:%d %s", (int)(stat.ctime%1000), ctime_r(&timestamp, szTmp));
  info += line;

  timestamp = stat.mtime/1000;
  snprintf(line, 1024, "mtime:%d %s", (int)(stat.mtime%1000), ctime_r(&timestamp, szTmp));
  info += line;

  snprintf(line, 1024, "version:%d\n", stat.version);
  info += line;

  snprintf(line, 1024, "cversion:%d\n", stat.cversion);
  info += line;

  snprintf(line, 1024, "aversion:%d\n", stat.aversion);
  info += line;

  snprintf(line, 1024, "dataLength:%d\n", stat.dataLength);
  info += line;

  snprintf(line, 1024, "numChildren:%d\n", stat.numChildren);
  info += line;

  snprintf(line, 1024, "pzxid:%s\n", toString(stat.pzxid, szTmp, 16));
  info += line;
}

///node创建事件
void ZkAdaptor::onNodeCreated(int , char const* )
{
}

///node删除事件
void ZkAdaptor::onNodeDeleted(int , char const* )
{
}

///node修改事件
void ZkAdaptor::onNodeChanged(int , char const* )
{
}

///node child修改事件
void ZkAdaptor::onNodeChildChanged(int , char const* )
{
}

///node 不再watch事件
void ZkAdaptor::onNoWatching(int , char const* )
{
}

void ZkAdaptor::onOtherEvent(int , int , const char *)
{
}
