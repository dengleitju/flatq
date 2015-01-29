#ifndef __ZK_ADAPTOR_H__
#define __ZK_ADAPTOR_H__

/**
*@file  CwxZkAdaptor.h
*@brief  Zookeeper的c++接口，采用Zookeeper的多线程模式
*@author cwinux@gmail.com
*@version 1.0
*@date
*@warning 无
*@bug
*/
#include <string>
#include <vector>
#include <list>
#include <map>
#include <inttypes.h>

using namespace std;

extern "C" {
#include "zookeeper.h"
}

class ZkAdaptor{
public:
  enum{
    ZK_DEF_RECV_TIMEOUT_MILISECOND = 5000  ///<通信超时时间，5s
  };
  enum{///<add auth状态
    AUTH_STATE_WAITING = 0,  ///<正在add auth
    AUTH_STATE_FAIL = 1,     ///<add auth失败
    AUTH_STATE_SUCCESS = 2   ///<add auth成功
  };
public:
  ///构造函数
  ZkAdaptor(string const& strHost, ///<zookeeper连接，为host:port结构
      uint32_t uiRecvTimeout=ZK_DEF_RECV_TIMEOUT_MILISECOND);
  ///析构函数
  virtual ~ZkAdaptor();
public:
  /**
  *@brief  对象初始化.
  *@param [in] level 错误日志级别，可以为
               ZOO_LOG_LEVEL_ERROR,ZOO_LOG_LEVEL_WARN,ZOO_LOG_LEVEL_INFO,ZOO_LOG_LEVEL_DEBUG
  *@return 0:success; -1:failure.
  */
  int init(ZooLogLevel level=ZOO_LOG_LEVEL_WARN);
  /**
  *@brief  建立连接，连接建立后会触发onConnect()。底层调用zookeeper_init()
           也可以通过isConnected()去轮询连接是否建立。
  *@param [in] clientid_t 连接的session
  *@param [in] flags zookeeper_init的flags参数，当前为0.
  *@param [in] watch zk的事件watch函数，若不指定则采用系统默认的watch，需要重载on的函数处理事件。
  *@param [in] context 若设定了watch，则需要指定watch的context。
  *@return 0:success; -1:failure.
  */
  virtual int connect(const clientid_t *clientid=NULL, int flags=0, watcher_fn watch=NULL, void *context=NULL);
  ///关闭连接
  void disconnect();
  ///检测连接是否建立。true：建立；false：未建立。
  bool isConnected()
  {
    if (m_zkHandle){
      int state = zoo_state (m_zkHandle);
      if (state == ZOO_CONNECTED_STATE) return true;
    }
    return false;
  }

  /**
  *@brief  对连接授权，底层调用zoo_add_auth()。
  *@param [in] scheme auth的scheme，当前只支持digest类型的授权
  *@param [in] cert auth的证书。对于digest模式，为user:passwd的格式
  *@param [in] certLen cert的长度。
  *@param [in] timeout 授权超时的时间，单位为ms。若为0则不等待，需要主动调用getAuthState()获取授权的结果。
  *@return true:授权成功；false：授权失败.
  */
  bool addAuth(const char* scheme, const char* cert, int certLen, uint32_t timeout=0, void_completion_t completion=NULL, const void *data=NULL);
  ///获取赋权状态，为AUTH_STATE_WAITING，AUTH_STATE_FAIL，AUTH_STATE_SUCCESS之一
  int getAuthState() const { return m_iAuthState;}

  /**
  *@brief  创建node，底层调用zoo_create()。
  *@param [in] path 节点路径
  *@param [in] data 节点的数据
  *@param [in] dataLen 数据的长度。
  *@param [in] acl 节点的访问权限。
  *@param [in] flags 节点的flag，0表示正常节点，或者为ZOO_SEQUENCE与ZOO_EPHEMERAL的组合。
  *@param [in] recursive  是否递归创建所有节点。
  *@param [out] pathBuf 若为ZOO_SEQUENCE类型的节点，返回真正的节点名字。
  *@param [in] pathBufLen  pathBuf的buf空间。
  *@return 1:成功；0:节点存在；-1：失败
  */
  int createNode(const string &path,
    char const* data,
    uint32_t dataLen,
    const struct ACL_vector *acl=&ZOO_OPEN_ACL_UNSAFE,
    int flags=0,
    bool recursive=false,
    char* pathBuf=NULL,
    uint32_t pathBufLen=0);
  /**
  *@brief  删除节点及其child节点。
  *@param [in] path 节点路径
  *@param [in] recursive 是否删除child node。
  *@param [in] version 数据版本，-1表示不验证版本号。
  *@return 1:成功；0：节点不存在；-1：失败.
  */
  int deleteNode(const string &path,
    bool recursive = false,
    int version = -1);
  /**
  *@brief  获取一个node的孩子。
  *@param [in] path 节点路径
  *@param [out] childs 节点的孩子列表
  *@param [in] watch 是否watch节点的变化。
  *@return 1:成功；0：节点不存在；-1：失败.
  */
  int getNodeChildren( const string &path, list<string>& childs, int watch=0);
  /**
  *@brief  检测一个节点是否存在。
  *@param [in] path 节点路径
  *@param [out] stat 节点的信息
  *@param [in] watch 是否watch节点的变化。
  *@return 1:存在；0：节点不存在；-1：失败.
  */
  int nodeExists(const string &path, struct Stat& stat, int watch=0);
  /**
  *@brief  获取一个节点的数据及信息。
  *@param [in] path 节点路径
  *@param [out] data 数据空间。
  *@param [in out]  dataLen 传入数据空间大小，传出data的真实大小。
  *@param [out] stat 节点信息。
  *@param [in] watch 是否watch节点的变化。
  *@return 1:成功；0：节点不存在；-1：失败.
  */
  int getNodeData(const string &path, char* data, uint32_t& dataLen, struct Stat& stat, int watch=0);
  /**
  *@brief  获取一个node的孩子，并注册事件函数。
  *@param [in] path 节点路径
  *@param [out] childs 节点的孩子列表
  *@param [in] watcher watch函数，若不指定则不watch。
  *@param [in] watcherCtx watch函数的context。
  *@return 1:成功；0：节点不存在；-1：失败.
  */
  int wgetNodeChildren( const string &path, list<string>& childs, watcher_fn watcher=NULL, void* watcherCtx=NULL);
  /**
  *@brief  检测一个节点是否存在。
  *@param [in] path 节点路径
  *@param [out] stat 节点的信息
  *@param [in] watcher watch函数，若不指定则不watch。
  *@param [in] watcherCtx watch函数的context。
  *@return 1:存在；0：节点不存在；-1：失败.
  */
  int wnodeExists(const string &path, struct Stat& stat, watcher_fn watcher=NULL, void* watcherCtx=NULL);
  /**
  *@brief  获取一个节点的数据及信息。
  *@param [in] path 节点路径
  *@param [out] data 数据空间。
  *@param [in out]  dataLen 传入数据空间大小，传出data的真实大小。
  *@param [out] stat 节点信息。
  *@param [in] watcher watch函数，若不指定则不watch。
  *@param [in] watcherCtx watch函数的context。
  *@return 1:成功；0：节点不存在；-1：失败.
  */
  int wgetNodeData(const string &path, char* data, uint32_t& dataLen, struct Stat& stat, watcher_fn watcher=NULL, void* watcherCtx=NULL);

  /**
  *@brief  获取一个节点的数据及信息。
  *@param [in] path 节点路径
  *@param [in] data 数据。
  *@param [in]  dataLen 数据大小。
  *@param [in] version 数据版本，若为-1表示不限定。
  *@return 1:成功；0：节点不存在；-1：失败.
  */
  int setNodeData(const string &path, char const* data, uint32_t dataLen, int version = -1);
  /**
  *@brief  获取一个节点的ACL信息。
  *@param [in] path 节点路径
  *@param [out] acl 节点的acl信息。
  *@param [out] stat 节点的统计信息。
  *@return 1:成功；0：节点不存在；-1：失败.
  */
  int getAcl(const char *path, struct ACL_vector& acl, struct Stat& stat);

  /**
  *@brief  设置一个节点的ACL信息。
  *@param [in] path 节点路径
  *@param [int] acl 节点的acl信息。
  *@param [in] version 数据版本，若为-1表示不限定。
  *@return 1:成功；0：节点不存在；-1：失败.
  */
  int setAcl(const char *path, const struct ACL_vector *acl=&ZOO_OPEN_ACL_UNSAFE, bool recursive=false, int version=-1);
  ///获取zookeeper的主机
  string const& getHost() const { return m_strHost;}
  ///获取zookeeper的连接handle
  zhandle_t* getZkHandle() { return m_zkHandle;}

  ///获取zookeeper连接的client session
  const clientid_t * getClientId() { return  isConnected()?zoo_client_id(m_zkHandle):NULL;}

  ///获取出错的错误代码
  int  getErrCode() const { return m_iErrCode;}

  ///获取错误消息
  char const* getErrMsg() const { return m_szErr2K;}
public:
  ///sleep miliSecond毫秒
  static void sleep(uint32_t miliSecond);
  ///split the src
  static int split(string const& src, list<string>& value, char ch);

  /**
  *@brief  对input的字符串进行base64的签名，用户需要释放返回的字符空间。
  *@param [in] input 要base64签名的字符串
  *@param [in] length input的长度。
  *@return NULL:失败；否则为input的base64签名
  */
  static char* base64(const unsigned char *input, int length);
  /**
  *@brief  对input的字符串进行sha1的签名。
  *@param [in] input 要sha1签名的字符串
  *@param [in] length input的长度。
  *@param [out] output 20byte的sha1签名值。
  *@return void
  */
  static void sha1(char const* input, int length, unsigned char *output);

  /**
  *@brief  对input的字符串进行sha1签名后，再进行base64变换。用户需要释放返回的空间
  *@param [in] input 要签名的字符串
  *@param [in] length input的长度。
  *@return NULL:失败；否则为input的base64签名
  */
  static char* digest(char const* input, int length);
  /**
  *@brief  根据priv形成acl。priv可以为all,self,read或者user:passwd:acrwd
  *@param [in] priv 要签名的字符串,可以为all,self,read或者user:passwd:acrwd
  *@param [in] acl  权限
  *@return false:失败；true:成功
  */
  static bool fillAcl(char const* priv, struct ACL& acl);


  ///输出权限信息，一个权限一个list的元素
  static void dumpAcl(ACL_vector const& acl, list<string>& info);

  ///输出节点的信息,一行一个信息项
  static void dumpStat(struct Stat const& stat, string& info);
public:
  ///事件通知，此时所有事件的根api
  virtual void onEvent(zhandle_t *t, int type, int state, const char *path);

  ///连接建立的回调函数，有底层的zk线程调用
  virtual void onConnect(){}

  ///正在建立联系的回调函数，有底层的zk线程调用
  virtual void onAssociating(){}

  ///正在建立连接的回调函数，有底层的zk线程调用
  virtual void onConnecting(){}

  ///鉴权失败的回调函数，有底层的zk线程调用
  virtual void onFailAuth(){}

  ///Session失效的回调函数，有底层的zk线程调用
  virtual void onExpired(){}
  /**
  *@brief  watch的node创建事件的回调函数，有底层的zk线程调用。应用于zoo_exists的watch。
  *@param [in] zk的watcher的state
  *@param [in] path watch的path.
  *@return void.
  */
  virtual void onNodeCreated(int state, char const* path);

  /**
  *@brief  watch的node删除事件的回调函数，有底层的zk线程调用。应用于 zoo_exists与zoo_get的watch。
  *@param [in] zk的watcher的state
  *@param [in] path watch的path.
  *@return void.
  */
  virtual void onNodeDeleted(int state, char const* path);

  /**
  *@brief  watch的node修改事件的回调函数，有底层的zk线程调用。应用于 zoo_exists与zoo_get的watch。
  *@param [in] zk的watcher的state
  *@param [in] path watch的path.
  *@return void.
  */
  virtual void onNodeChanged(int state, char const* path);
  /**
  *@brief  watch的node孩子变更事件的回调函数，有底层的zk线程调用。应用于zoo_get_children的watch。
  *@param [in] zk的watcher的state
  *@param [in] path watch的path.
  *@return void.
  */
  virtual void onNodeChildChanged(int state, char const* path);

  /**
  *@brief  zk取消某个wathc的通知事件的回调函数，有底层的zk线程调用。
  *@param [in] zk的watcher的state
  *@param [in] path watch的path.
  *@return void.
  */
  virtual void onNoWatching(int state, char const* path);

  /**
  *@brief  其他zookeeper的事件通知回调函数，有底层的zk线程调用。
  *@param [in] type的watcher的事件type
  *@param [in] zk的watcher的state
  *@param [in] path watch的path.
  *@return void.
  */
  virtual void onOtherEvent(int type, int state, const char *path);
private:
  ///内部的wacher function
  static void watcher(zhandle_t *zzh, int type, int state, const char *path,
      void* context);

  ///内部add auth的function
  static void authCompletion(int rc, const void *data);
private:
  string         m_strHost; ///<The host addresses of ZK nodes.
  uint32_t       m_uiRecvTimeout; ///<消息接收超时
  int            m_iAuthState; ///<add auth的完成状态
  zhandle_t*     m_zkHandle;    ///<The current ZK session.
  int            m_iErrCode;   ///<Err code
  char           m_szErr2K[2048];  ///<Err msg
};

#endif
