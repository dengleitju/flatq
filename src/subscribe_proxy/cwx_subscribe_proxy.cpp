#include "CwxAppProcessMgr.h"
#include "CwxSubProxyApp.h"

int main(int argc, char** argv) {
  ///创建CwxMqApp的app对象实例
  CwxSubProxyApp* pApp = new CwxSubProxyApp();
  //初始化双进程管理器
  if (0 != CwxAppProcessMgr::init(pApp))
    return 1;
  //启动双进程，一个为监控mq进程的监控进程，一个为提供mq服务的工作进程。
  CwxAppProcessMgr::start(argc, argv, 200, 300);
}

