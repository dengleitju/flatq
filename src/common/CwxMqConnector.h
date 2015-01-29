#ifndef __CWX_MQ_CONNECTOR_H__
#define __CWX_MQ_CONNECTOR_H__

#include "CwxPre.h"
#include "CwxType.h"
#include "CwxErrGuard.h"
#include "CwxNetMacro.h"
#include "CwxINetAddr.h"
#include "CwxSockStream.h"
#include "CwxSockBase.h"
#include "CwxAppMacro.h"

CWINUX_USING_NAMESPACE

///在timeout指定的时间内，建立addr指向的unConnNum条连接，只要一条失败则都失败
class CwxMqConnector {
public:
  ///建立连接。返回值：0：成功；-1：失败。
  static int connect(CwxINetAddr const& addr, ///<连接的地址
    CWX_UINT16 unConnNum, ///<连接的数量
    int* fd, ///<返回fd的数值，其空间有外部保证。
    CwxTimeouter* timeout = NULL, ///<连接超时时间
    bool reuse_addr = false, ///<是否重用端口
    CWX_NET_SOCKET_ATTR_FUNC fn = NULL, ///<socket设置的function
    void* fnArg = NULL ///<socket设置function的参数
    );
private:
  ///等待连接完成并处理
  static int complete(set<int>& fds, ///<处理的连接集合
    CwxTimeouter* timeout ///<超时时间
    );
private:
  ///默认构造函数
  CwxMqConnector() {
  }
  ///析构函数.
  ~CwxMqConnector(void) {
  }
};

#endif
