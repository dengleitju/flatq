#include "CwxProductProxyApp.h"
#include  "CwxDate.h"


///构造函数
CwxProProxyApp::CwxProProxyApp() {
  m_ttCurTime = 0;
  memset(m_szBuf, 0x00, MAX_MONITOR_REPLY_SIZE);
  m_zkHandler = NULL;
  m_zkThreadPool = NULL;
  m_mqHandler = NULL;
  m_recvHandler = NULL;
  m_mqThreadPool = NULL;
}

///析构函数
CwxProProxyApp::~CwxProProxyApp() {
}

///初始化
int CwxProProxyApp::init(int argc, char** argv) {
  string strErrMsg;
  ///首先调用框架的init api
  if (CwxAppFramework::init(argc, argv) == -1)
    return -1;
  ///检查是否通过-f执行了配置文件，如没有则采用默认配置文件
  if ((NULL == this->getConfFile()) || (strlen(this->getConfFile()) == 0)) {
    this->setConfFile("product_proxy.conf");
  }

  ///加载配置文件，若失败则退出
  if (0 != m_config.loadConfig(getConfFile())) {
    CWX_ERROR((m_config.getErrMsg()));
    return -1;
  }
  ///设置运行日志的level
  setLogLevel(CwxLogger::LEVEL_ERROR | CwxLogger::LEVEL_INFO | CwxLogger::LEVEL_WARNING);
  return 0;
}

///配置运行环境信息
int CwxProProxyApp::initRunEnv() {
  ///设置系统的时间间隔，最小时刻为1ms，此时为0.1s
  this->setClick(1000); ///1s
  ///设置工作目录
  this->setWorkDir(m_config.getCmmon().m_strWorkDir.c_str());
  ///设置循环运行的日志数量
  this->setLogFileNum(LOG_FILE_NUM);
  ///设置每个日志文件的大小
  this->setLogFileSize(LOG_FILE_SIZE * 1024 * 1024);
  ///调用架构的initRunEnv,是以上参数生效
  if (CwxAppFramework::initRunEnv() == -1) return -1;
  ///将加载的配置文件信息输出到日志文件
  m_config.outputConfig();
  ///block各种signal
  this->blockSignal(SIGTERM);
  this->blockSignal(SIGUSR1);
  this->blockSignal(SIGUSR2);
  this->blockSignal(SIGCHLD);
  this->blockSignal(SIGCLD);
  this->blockSignal(SIGHUP);
  this->blockSignal(SIGPIPE);
  this->blockSignal(SIGALRM);
  this->blockSignal(SIGCONT);
  this->blockSignal(SIGSTOP);
  this->blockSignal(SIGTSTP);
  this->blockSignal(SIGTTOU);

  //set version
  this->setAppVersion(CWX_PRODUCT_PROXY_VERSION);
  //set last modify date
  this->setLastModifyDatetime(CWX_PRODUCT_PROXY_MODIFY);
  //set compile date
  this->setLastCompileDatetime(CWX_COMPILE_DATE(_BUILD_DATE));
  ///设置启动时间
  CwxDate::getDateY4MDHMS2(time(NULL), m_strStartTime);

  ///创建mq的消息的处理handle
  m_mqHandler = new CwxProProxyMqHandler(this);
  ///注册handele
  getCommander().regHandle(SVR_PRO_TYPE_MQ, m_mqHandler);
  ///创建代理消息的处理handle
  m_recvHandler = new CwxProProxyRecvHandler(this);
  getCommander().regHandle(SVR_PRO_TYPE_RECV, m_recvHandler);
  ///创建消息处理线程池
  m_mqThreadPool = new CwxThreadPool(1,
      &getCommander());
  ///创建线程的tss对象
  CwxTss** pTss = new CwxTss*[1];
  pTss[0] = new CwxMqTss();
  ((CwxMqTss*)pTss[0])->init();
  ///启动线程
  if (0 != m_mqThreadPool->start(pTss)) {
    CWX_ERROR(("Failure to start mq thread pool."));
    return -1;
  }

  ///启动zk线程池
  m_zkThreadPool = new CwxThreadPool(1,
      &getCommander(),
      CwxProProxyApp::zkThreadMain,
      this);
  pTss = new CwxTss*[1];
  pTss[0] = new CwxMqTss();
  ((CwxMqTss*)pTss[0])->init();
  if (0 != m_zkThreadPool->start(pTss)) {
    CWX_ERROR(("Failure to start zookeeper thread pool."));
    return -1;
  }
  ///创建zk的handler
  m_zkHandler = new CwxProProxyZkHandler(this,
      m_config.getZk().m_strRootPath,
      m_config.getZk().m_strZkServer
      );
  if (0 != m_zkHandler->init()) {
    CWX_ERROR(("Failure to init zk handler."));
    return -1;
  }
  ///打开监听端口
  if(0 > this->noticeTcpListen(SVR_PRO_TYPE_RECV,
      m_config.getCmmon().m_listen.getHostName().c_str(),
      m_config.getCmmon().m_listen.getPort(),
      false,
      CWX_APP_MSG_MODE,
      CwxProProxyApp::setRecvSockAttr,
      this)) {
    CWX_ERROR(("Failure to open listen host:%s port:%d",
        m_config.getCmmon().m_listen.getHostName().c_str(),
        m_config.getCmmon().m_listen.getPort()));
    return -1;
  }
  return 0;
}

///zk消息处理handler
void* CwxProProxyApp::zkThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg) {
  CwxProProxyApp* app = (CwxProProxyApp*)arg;
  CwxMsgBlock* block = NULL;
  int ret = 0;
  while (-1 != (ret = queue->dequeue(block))) {
    app->getZkhandler()->doEvent(tss, block, ret);
    if (block) CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (!app->isStopped()) {
    CWX_ERROR(("Stop app for zk thread is stopped."));
    app->stop();
  }
  return NULL;
}

///时钟函数
void CwxProProxyApp::onTime(CwxTimeValue const& current) {
  ///调用基类的onTime函数
  CwxAppFramework::onTime(current);
  m_ttCurTime = current.sec();
  ///检查超时
  static CWX_UINT32 ttTimeBase = 0; ///<时钟回跳的base时钟
  static CWX_UINT32 ttLastTime = 0; ///<上次检查时间
  bool bClockBack = isClockBack(ttTimeBase, m_ttCurTime);
  if (bClockBack || (m_ttCurTime >= ttLastTime + 1)) {
    ttLastTime = m_ttCurTime;
    if (m_zkThreadPool) { ///发送超时检查到zk线程
      CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_PRO_TYPE_ZK);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      m_zkThreadPool->append(pBlock);
    }
    if (m_mqThreadPool) { ///发送超时检查到mq线程
      CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_PRO_TYPE_RECV);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      m_mqThreadPool->append(pBlock);
    }
  }
}

///信号处理函数
void CwxProProxyApp::onSignal(int signum) {
  switch (signum) {
    case SIGQUIT:
      ///若监控进程通知退出，则推出
      CWX_INFO(("Recv exit signal, exit right now."));
      this->stop();
      break;
    default:
      ///其他信号，全部忽略
      CWX_INFO(("Recv signal=%d, ignore it.", signum));
      break;
  }
}

///连接建立
int CwxProProxyApp::onConnCreated(CwxAppHandler4Msg& ,
                            bool&,
                            bool&)
{
  return 0;
}

///连接关闭
int CwxProProxyApp::onConnClosed(CwxAppHandler4Msg& ,
    CwxAppHandler4Msg& conn,
    CwxMsgHead const& ,
    bool& ) {
  CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
  pBlock->event().setSvrId(conn.getConnInfo().getSvrId());
  pBlock->event().setHostId(conn.getConnInfo().getHostId());
  pBlock->event().setConnId(conn.getConnInfo().getConnId());
  pBlock->event().setEvent(CwxEventInfo::CONN_CLOSED);
  m_mqThreadPool->append(pBlock);
  return 0;
}

///发送失败
void CwxProProxyApp::onFailSendMsg(CwxMsgBlock*& msg) {
  if (SVR_PRO_TYPE_MQ == msg->send_ctrl().getSvrId()) {
    msg->event().setSvrId(SVR_PRO_TYPE_MQ);
    msg->event().setHostId(msg->send_ctrl().getHostId());
    msg->event().setConnId(msg->send_ctrl().getConnId());
    msg->event().setEvent(CwxEventInfo::FAIL_SEND_MSG);
    m_mqThreadPool->append(msg);
    msg = NULL;
  }
}

///收到消息
int CwxProProxyApp::onRecvMsg(CwxMsgBlock* msg,
                        CwxAppHandler4Msg& conn,
                        CwxMsgHead const& header,
                        bool&)
{
  msg->event().setSvrId(conn.getConnInfo().getSvrId());
  msg->event().setHostId(conn.getConnInfo().getHostId());
  msg->event().setConnId(conn.getConnInfo().getConnId());
  msg->event().setEvent(CwxEventInfo::RECV_MSG);
  msg->event().setMsgHeader(header);
  msg->event().setTimestamp(CwxDate::getTimestamp());
  m_mqThreadPool->append(msg);
  return 0;
}


void CwxProProxyApp::destroy() {
  ///停止zk线程，必须先于zkHandler->stop,否则死锁
  if (m_zkThreadPool) m_zkThreadPool->stop();
  if (m_mqThreadPool) m_mqThreadPool->stop();

  if (m_mqHandler) {
    delete m_mqHandler;
    m_mqHandler = NULL;
  }

  if (m_recvHandler) {
    delete m_recvHandler;
    m_recvHandler = NULL;
  }
  ///停止zookeeper底层线程
  if (m_zkHandler) {
    m_zkHandler->stop();
    delete m_zkHandler;
    m_zkHandler = NULL;
  }
  ///删除zk线程
  if (m_zkThreadPool) delete m_zkThreadPool;
  m_zkThreadPool = NULL;
  if (m_mqThreadPool) delete m_mqThreadPool;
  m_mqThreadPool = NULL;
  CwxAppFramework::destroy();
}

///设置recv连接的属性
int CwxProProxyApp::setRecvSockAttr(CWX_HANDLE handle, void* arg)
{
  CwxProProxyApp* app = (CwxProProxyApp*)arg;

  if (0 != CwxSocket::setKeepalive(handle,
      true,
      CWX_APP_DEF_KEEPALIVE_IDLE,
      CWX_APP_DEF_KEEPALIVE_INTERNAL,
      CWX_APP_DEF_KEEPALIVE_COUNT))
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
      app->m_config.getCmmon().m_listen.getHostName().c_str(),
      app->m_config.getCmmon().m_listen.getPort(),
      errno));
    return -1;
  }

  int flags= 1;
  if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
        app->m_config.getCmmon().m_listen.getHostName().c_str(),
        app->m_config.getCmmon().m_listen.getPort(),
        errno));
    return -1;
  }
  struct linger ling= {0, 0};
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
  {
      CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
          app->m_config.getCmmon().m_listen.getHostName().c_str(),
          app->m_config.getCmmon().m_listen.getPort(),
          errno));
      return -1;
  }
  return 0;
}

///设置mq连接的属性
int CwxProProxyApp::setMqSockAttr(CWX_HANDLE handle, void* )
{
  if (0 != CwxSocket::setKeepalive(handle,
      true,
      CWX_APP_DEF_KEEPALIVE_IDLE,
      CWX_APP_DEF_KEEPALIVE_INTERNAL,
      CWX_APP_DEF_KEEPALIVE_COUNT))
  {
    CWX_ERROR(("Failure to set  mq keep-alive, errno=%d", errno));
    return -1;
  }
  int flags= 1;
  if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags)) != 0)
  {
    CWX_ERROR(("Failure to set mq NODELAY, errno=%d",errno));
    return -1;
  }
  struct linger ling= {0, 0};
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling)) != 0)
  {
    CWX_ERROR(("Failure to set mqLINGER, errno=%d",errno));
    return -1;
  }
  return 0;
}
