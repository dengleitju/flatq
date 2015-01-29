#include "CwxMqApp.h"
#include "CwxDate.h"

///构造函数
CwxMqApp::CwxMqApp() {
  m_uiCurSid = 0;
  m_uiCurTimestamp = 0;
  m_ttCurTime = 0;
  m_ullStartSid = 0;
  m_uiTopicFileModifyTime = 0;
  m_pTopicMgr = NULL;
  m_masterHandler = NULL;
  m_recvHandler = NULL;
  m_zkHandler = NULL;
  m_zkThreadPool = NULL;
  m_recvThreadPool = NULL;
  m_innerDispatchThreadPool = NULL;
  m_innerDispatchChannel = NULL;
  m_outerDispatchThreadPool = NULL;
  m_outerDispatchChannel = NULL;
  memset(m_szBuf, 0x00, MAX_MONITOR_REPLY_SIZE);
}

///析构函数
CwxMqApp::~CwxMqApp() {
}

///初始化
int CwxMqApp::init(int argc, char** argv) {
  string strErrMsg;
  ///首先调用架构的init api
  if (CwxAppFramework::init(argc, argv) == -1)
    return -1;
  ///检查是否通过-f指定了配置文件，若没有，则采用默认的配置文件
  if ((NULL == this->getConfFile()) || (strlen(this->getConfFile()) == 0)) {
    this->setConfFile("mq.conf");
  }
  ///加载配置文件，若失败则退出
  if (0 != m_config.loadConfig(getConfFile())) {
    CWX_ERROR((m_config.getErrMsg()));
    return -1;
  }
  ///设置运行日志的level
  if (m_config.getCommon().m_bDebug) {
    setLogLevel(CwxLogger::LEVEL_ERROR | CwxLogger::LEVEL_INFO | CwxLogger::LEVEL_WARNING
        | CwxLogger::LEVEL_DEBUG);
  }else {
    setLogLevel(CwxLogger::LEVEL_ERROR | CwxLogger::LEVEL_INFO | CwxLogger::LEVEL_WARNING);
  }
  return 0;
}


///配置运行环境信息
int CwxMqApp::initRunEnv() {
  ///设置系统的时钟间隔，最小时刻为1ms,此为0.1s
  this->setClick(100); //0.1s
  ///设置工作目录
  this->setWorkDir(m_config.getCommon().m_strWorkDir.c_str());
  ///设置循环运行日志的数量
  this->setLogFileNum(LOG_FILE_NUM);
  ///设置每个日志文件的大小
  this->setLogFileSize(LOG_FILE_SIZE * 1024 * 1024);
  ///调用架构的initRunEnv,使以上设置的参数生效
  if (CwxAppFramework::initRunEnv() == -1) return -1;
  ///将加载的配置文件信息输出到日志文件中，供查看检查
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
  this->setAppVersion(CWX_MQ_VERSION);
  //set last modify date
  this->setLastModifyDatetime(CWX_MQ_MODIFY_DATE);
  //set compile date
  this->setLastCompileDatetime(CWX_COMPILE_DATE(_BUILD_DATE));
  ///设置启动时间
  CwxDate::getDateY4MDHMS2(time(NULL), m_strStartTime);

  int ret = -1;
  do {
    ///启动binlog管理器
    if (0 != startBinLogMgr()) break;

    ///启动网络连接与监听
    if (0 != startNetwork()) break;

    ///注册数据接收handler
    m_recvHandler = new CwxMqRecvHandler(this);
    getCommander().regHandle(SVR_TYPE_RECV, m_recvHandler);

    ///注册处理主从同步handler
    m_masterHandler = new CwxMqMasterHandler(this);
    getCommander().regHandle(SVR_TYPE_MASTER, m_masterHandler);

    ///创建recv/master线程池对象
    ///线程池的数量为1
    m_recvThreadPool = new CwxThreadPool(1, &getCommander());
    ///创建线程的tss对象
    CwxTss** pTss = new CwxTss*[1];
    pTss[0] = new CwxMqTss();
    ((CwxMqTss*)pTss[0])->init();
    ///启动线程
    if (0 != m_recvThreadPool->start(pTss)) {
      CWX_ERROR(("Failure to start recv thread pool"));
      break;
    }

    ///创建内部分发线程池
    m_innerDispatchChannel = new CwxAppChannel();
    m_innerDispatchThreadPool = new CwxThreadPool(1,
        &getCommander(),
        CwxMqApp::innerDispatchThreadMain,
        this);
    ///启动线程
    pTss = new CwxTss*[1];
    pTss[0] = new CwxMqTss();
    ((CwxMqTss*)pTss[0])->init();
    if (0 != m_innerDispatchThreadPool->start(pTss)) {
      CWX_ERROR(("Failure to start inner dispatch thread pool"));
      break;
    }

    ///创建外部分发线程池
    m_outerDispatchChannel = new CwxAppChannel();
    m_outerDispatchThreadPool = new CwxThreadPool(1,
        &getCommander(),
        CwxMqApp::outerDispatchThreadMain,
        this);
    ///启动线程
    pTss = new CwxTss*[1];
    pTss[0] = new CwxMqTss();
    ((CwxMqTss*)pTss[0])->init();
    if (0 != m_outerDispatchThreadPool->start(pTss)) {
      CWX_ERROR(("Failure to start outer dispatch thread pool"));
      break;
    }

    if (m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_ZK) {
      ///启动zk线程池
      m_zkThreadPool = new CwxThreadPool(1,
          &getCommander(),
          CwxMqApp::zkThreadMain,
          this);
      if (0 != m_zkThreadPool->start(NULL)) {
        CWX_ERROR(("Failure to start zookeeper thread pool."));
        break;
      }
      ///创建zk的handler
      m_zkHandler = new CwxMqZkHandler(this);
      if (0 != m_zkHandler->init()) {
        CWX_ERROR(("Failure to init zk handler"));
      }
    }
    ret = 0;
  } while(0);
  if (0 != ret) {
    this->blockSignal(SIGQUIT);
  }
  if (m_config.getCommon().m_type != CwxMqConfigCmn::MQ_TYPE_ZK){
    ///创建source目录
    if (!CwxFile::isDir(m_config.getOuterDispatch().m_strSourcePath.c_str())) {
      if (!CwxFile::createDir(m_config.getOuterDispatch().m_strSourcePath.c_str())) {
        CWX_ERROR(("Failure to create source path:%s, errno=%d",
            m_config.getOuterDispatch().m_strSourcePath.c_str(),
            errno));
        return -1;
      }
      CWX_INFO(("Success to create source path:%s", m_config.getOuterDispatch().m_strSourcePath.c_str()));
    }
  }
  return ret;
}

int CwxMqApp::startNetwork() {
  ///打开监听的服务端口
  if (m_config.getCommon().m_monitor.getHostName().length()) {
    if (0 > this->noticeTcpListen(SVR_TYPE_MONITOR,
        m_config.getCommon().m_monitor.getHostName().c_str(),
        m_config.getCommon().m_monitor.getPort(), true)) {
      CWX_ERROR(("Can't register the monitor tcp accept listen: addr=%s, port=%d",
          m_config.getCommon().m_monitor.getHostName().c_str(),
          m_config.getCommon().m_monitor.getPort()));
      return -1;
    }
  }
  ///打开外部分发端口
  if (0 > this->noticeTcpListen(SVR_TYPE_OUTER_DISP,
      m_config.getOuterDispatch().m_async.getHostName().c_str(),
      m_config.getOuterDispatch().m_async.getPort(),
      false,
      CWX_APP_EVENT_MODE,
      CwxMqApp::setDispatchSockAttr, this)) {
    CWX_ERROR(("Can't register the out async-dispatch tcp accept listen: add=%s, port=%d",
        m_config.getOuterDispatch().m_async.getHostName().c_str(),
        m_config.getOuterDispatch().m_async.getPort()));
    return -1;
  }
  ///打开内部分发端口
  if (0 > this->noticeTcpListen(SVR_TYPE_INNER_DISP,
      m_config.getInnerDispatch().m_async.getHostName().c_str(),
      m_config.getInnerDispatch().m_async.getPort(),
      false,
      CWX_APP_EVENT_MODE,
      CwxMqApp::setDispatchSockAttr, this)) {
    CWX_ERROR(("Can't register the inner async-dispatch tcp accept listen: add=%s, port=%d",
        m_config.getInnerDispatch().m_async.getHostName().c_str(),
        m_config.getInnerDispatch().m_async.getPort()));
    return -1;
  }
  ///消息接收端口
  if (m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_MASTER ||
      m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_ZK) { //zk/master版打开recv端口
    if (0 > this->noticeTcpListen(SVR_TYPE_RECV,
        m_config.getRecv().m_recv.getHostName().c_str(),
        m_config.getRecv().m_recv.getPort(),
        false,
        CWX_APP_MSG_MODE,
        CwxMqApp::setMasterRecvSockAttr, this)) {
      CWX_ERROR(("Can't register the recv tcp accept listen: add=%s, port=%d",
          m_config.getRecv().m_recv.getHostName().c_str(),
          m_config.getRecv().m_recv.getPort()));
      return -1;
    }
  }
  return 0;
}

///启动binlog的管理器，-1：失败；0：成功
int CwxMqApp::startBinLogMgr() {
  ///初始化binlog
  {
    CWX_UINT64 ullBinLogSize = m_config.getBinLog().m_uiBinLogMSize;
    ullBinLogSize *= 1024 * 1024;
    if (ullBinLogSize > CwxBinLogMgr::MAX_BINLOG_FILE_SIZE) ullBinLogSize = CwxBinLogMgr::MAX_BINLOG_FILE_SIZE;
    m_pTopicMgr = new CwxTopicMgr(m_config.getBinLog().m_strBinlogPath.c_str());
    if (0 != m_pTopicMgr->init(m_uiCurSid,
        m_uiCurTimestamp,
        m_ullStartSid,
        m_config.getBinLog().m_uiBinLogMSize,
        m_config.getBinLog().m_uiFlushNum,
        m_config.getBinLog().m_uiFlushSecond,
        m_config.getBinLog().m_uiMgrFileNum,
        m_config.getBinLog().m_uiSaveFileDay,
        true,
        CWX_TSS_2K_BUF)) {
      CWX_ERROR(("Failure to init binlog topic mgr:%s", CWX_TSS_2K_BUF));
      return -1;
    }
    char szSid1[64];
    char szSid2[64];
    CWX_INFO(("Init binlog topic mgr success. start_sid:%s, max_sid:%s",
        CwxCommon::toString(m_ullStartSid, szSid1, 10),
        CwxCommon::toString(m_uiCurSid, szSid2, 10)));
  } ///成功
  m_strMaxSidFile = m_config.getBinLog().m_strBinlogPath + "binlog_max_sid";
  if ( m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_ZK) { ///读去sid文件
    string strData;
    if (CwxFile::readTxtFile(m_strMaxSidFile, strData)){
      CWX_UINT64 ullSid = strtoull(strData.c_str(), NULL, 10);
      if (m_uiCurSid < ullSid) m_uiCurSid = ullSid;
    }
  }
  return 0;
}
///设置master recv连接的属性
int CwxMqApp::setMasterRecvSockAttr(CWX_HANDLE handle,
                                    void* arg)
{
  CwxMqApp* app = (CwxMqApp*) arg;
  if (0 != CwxSocket::setKeepalive(handle,
    true,
    CWX_APP_DEF_KEEPALIVE_IDLE,
    CWX_APP_DEF_KEEPALIVE_INTERNAL,
    CWX_APP_DEF_KEEPALIVE_COUNT))
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
      app->getConfig().getRecv().m_recv.getHostName().c_str(),
      app->getConfig().getRecv().m_recv.getPort(), errno));
    return -1;
  }

  int flags = 1;
  if (setsockopt(handle,
    IPPROTO_TCP,
    TCP_NODELAY,
    (void *) &flags,
    sizeof(flags)) != 0)
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
      app->getConfig().getRecv().m_recv.getHostName().c_str(),
      app->getConfig().getRecv().m_recv.getPort(),
      errno));
    return -1;
  }
  struct linger ling = { 0, 0 };
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
      app->getConfig().getRecv().m_recv.getHostName().c_str(),
      app->getConfig().getRecv().m_recv.getPort(), errno));
    return -1;
  }
  return 0;
}

///设置inner dispatch连接的属性
int CwxMqApp::setDispatchSockAttr(CWX_HANDLE handle, void* arg) {
  CwxMqApp* app = (CwxMqApp*) arg;
  if (app->getConfig().getCommon().m_uiSockBufSize) {
    int iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023) / 1024;
    iSockBuf *= 1024;
    while (setsockopt(handle, SOL_SOCKET, SO_SNDBUF, (void*) &iSockBuf,
      sizeof(iSockBuf)) < 0)
    {
      iSockBuf -= 1024;
      if (iSockBuf <= 1024) break;
    }
    iSockBuf = (app->getConfig().getCommon().m_uiSockBufSize + 1023) / 1024;
    iSockBuf *= 1024;
    while (setsockopt(handle, SOL_SOCKET, SO_RCVBUF, (void *) &iSockBuf,
      sizeof(iSockBuf)) < 0)
    {
      iSockBuf -= 1024;
      if (iSockBuf <= 1024) break;
    }
  }

  if (0!= CwxSocket::setKeepalive(handle,
    true,
    CWX_APP_DEF_KEEPALIVE_IDLE,
    CWX_APP_DEF_KEEPALIVE_INTERNAL,
    CWX_APP_DEF_KEEPALIVE_COUNT))
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
      app->getConfig().getInnerDispatch().m_async.getHostName().c_str(),
      app->getConfig().getInnerDispatch().m_async.getPort(), errno));
    return -1;
  }


  int flags = 1;
  if (setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, (void *) &flags,
    sizeof(flags)) != 0)
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
      app->getConfig().getInnerDispatch().m_async.getHostName().c_str(),
      app->getConfig().getInnerDispatch().m_async.getPort(), errno));
    return -1;
  }
  struct linger ling = { 0, 0 };
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
      app->getConfig().getInnerDispatch().m_async.getHostName().c_str(),
      app->getConfig().getInnerDispatch().m_async.getPort(), errno));
    return -1;
  }
  return 0;
}

///外部分发channel的线程函数，arg为app对象
void* CwxMqApp::outerDispatchThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg) {
  CwxMqApp* app = (CwxMqApp*)arg;
  if (0 != app->getOuterDispChannel()->open()) {
    CWX_ERROR(("Failure to open out dispatch channel."));
    return NULL;
  }
  while(1) {
    ///获取队列中的消息处理
    if (0 != dealOuterDispatchThreadQueueMsg(tss,
        queue,
        app,
        app->getOuterDispChannel())) {
      break;
    }
    if (-1 == app->getOuterDispChannel()->dispatch(1)) {
      CWX_ERROR(("Failure to CwxAppChannel::dispatch()"));
      sleep(1);
    }
    ///检查关闭的连接
    CwxMqOuterDispHandler::dealClosedSession(app, (CwxMqTss*)tss);
  }
  ///释放分发的资源
  CwxMqOuterDispHandler::destroy(app);
  app->getOuterDispChannel()->stop();
  app->getOuterDispChannel()->close();
  if (!app->isStopped()) {
    CWX_INFO(("Stop app for out dispatch channel thread stopped."));
    app->stop();
  }
  return NULL;
}

///外部分发channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxMqApp::dealOuterDispatchThreadQueueMsg(CwxTss* tss,
                                         CwxMsgQueue* queue,
                                         CwxMqApp* app,
                                         CwxAppChannel*) {
  int iRet = 0;
  CwxMsgBlock* block = NULL;
  while (!queue->isEmpty()) {
    iRet = queue->dequeue(block);
    if (-1 == iRet) return -1;
    CWX_ASSERT(block->event().getSvrId() == SVR_TYPE_OUTER_DISP);
    CwxMqOuterDispHandler::doEvent(app, (CwxMqTss*)tss, block);
    if (block) CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (queue->isDeactived()) return -1;
  return 0;
}

///内部channel的线程函数，arg为app对象
void* CwxMqApp::innerDispatchThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg) {
  CwxMqApp* app = (CwxMqApp*)arg;
  if (0 != app->getInnerDispChannel()->open()) {
    CWX_ERROR(("Failure to open out dispatch channel."));
    return NULL;
  }
  while(1) {
    ///获取队列中的消息处理
    if (0 != dealInnerDispatchThreadQueueMsg(tss,
        queue,
        app,
        app->getInnerDispChannel())) {
      break;
    }
    if (-1 == app->getInnerDispChannel()->dispatch(1)) {
      CWX_ERROR(("Failure to CwxAppChannel::dispatch()"));
      sleep(1);
    }
    ///检查关闭的连接
    CwxMqInnerDispHandler::dealClosedSession(app, (CwxMqTss*)tss);
  }
  ///释放分发的资源
  CwxMqInnerDispHandler::destroy(app);
  app->getInnerDispChannel()->stop();
  app->getInnerDispChannel()->close();
  if (!app->isStopped()) {
    CWX_INFO(("Stop app for out dispatch channel thread stopped."));
    app->stop();
  }
  return NULL;
}

///内部分发channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxMqApp::dealInnerDispatchThreadQueueMsg(CwxTss* tss,
                                         CwxMsgQueue* queue,
                                         CwxMqApp* app,
                                         CwxAppChannel*) {
  int iRet = 0;
  CwxMsgBlock* block = NULL;
  while (!queue->isEmpty()) {
    iRet = queue->dequeue(block);
    if (-1 == iRet) return -1;
    CWX_ASSERT(block->event().getSvrId() == SVR_TYPE_INNER_DISP);
    CwxMqInnerDispHandler::doEvent(app, (CwxMqTss*)tss, block);
    if (block) CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (queue->isDeactived()) return -1;
  return 0;
}

///zk消息处理handler
void* CwxMqApp::zkThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg) {
  CwxMqApp* app = (CwxMqApp*)arg;
  CwxMsgBlock* block = NULL;
  int ret = 0;
  while (-1 != (ret = queue->dequeue(block))) {
    app->getZkHandler()->doEvent(tss, block, ret);
    if (block) CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (!app->isStopped()) {
    CWX_INFO(("Stop app for zk thread is stopped."));
    app->stop();
  }
  return NULL;
}

///时钟函数
void CwxMqApp::onTime(CwxTimeValue const& current) {
  ///调用基类的onTime函数
  CwxAppFramework::onTime(current);
  m_ttCurTime = current.sec();
  ///检查超时
  static CWX_UINT32 ttTimeBase = 0; ///<时钟回跳的base时钟
  static CWX_UINT32 ttLastTime = 0; ///<上一次检查的时间
  bool bClockBack = isClockBack(ttTimeBase, m_ttCurTime);
  if (bClockBack || (m_ttCurTime >= ttLastTime + 1)) {
    ttLastTime = m_ttCurTime;
    if (m_zkThreadPool){///发送超时检查到zk线程
      CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_TYPE_ZK);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      m_zkThreadPool->append(pBlock);
    }
    if (m_recvThreadPool){///发送超时检查到recv线程
      CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_TYPE_RECV);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      m_recvThreadPool->append(pBlock);
    }
    if (m_outerDispatchThreadPool){ ///发送超时检查到内部分发线程
      CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_TYPE_OUTER_DISP);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      m_outerDispatchThreadPool->append(pBlock);
    }
    if (m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_MASTER) ///检测topic配置文件变化
      checkTopicConfModify();
  }
  ///检查topic变化
  static CWX_UINT32 ttTopicLastTime = 0; ///上次检测时间
  if (bClockBack || (m_ttCurTime >= ttTopicLastTime + 600)) ///10分钟检测一次
  {
    ttTopicLastTime = m_ttCurTime;
    ///检测被删除的binlogMgr
    list<string> delTopics;
    m_pTopicMgr->checkTimeoutBinlogMgr(delTopics, m_ttCurTime);
    if (delTopics.size()) { ///清空改topic对应的source信息
      if (m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_ZK) saveMaxSid();
      CwxWriteLockGuard<CwxRwLock> lock(&m_rwSourceLock);
      for(list<string>::iterator iter = delTopics.begin(); iter != delTopics.end(); iter++) {
        m_topicSource.erase(*iter);
      }
    }
  }
}

///加载topic配置文件
int CwxMqApp::loadTopicChange(bool bForceLoad) {
  string strFile = m_config.getCommon().m_strTopicConf;
  CWX_UINT32 uiModifyTime = CwxFile::getFileMTime(strFile.c_str());
  if (0 == uiModifyTime) {
    CWX_ERROR(("Failure to get topic conf file:%s, errno=%d", strFile.c_str(), errno));
    return -1;
  }
  if ((uiModifyTime != m_uiTopicFileModifyTime) || bForceLoad) {
    if (0 != m_config.loadTopic(strFile)) {
      CWX_ERROR(("Failure to load topic file:%s, errno=%s",
          strFile.c_str(), m_config.getErrMsg()));
      return -1;
    }
    m_uiTopicFileModifyTime = uiModifyTime;
    if (!m_recvHandler->isCanWrite()) m_recvHandler->setCanWrite();
    return 1;
  }
  return 0;
}

///读取topic配置文件，检测新增/删除的topic
void CwxMqApp::checkTopicConfModify() {
  CWX_ASSERT(m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_MASTER);
  int ret = this->loadTopicChange(false);
  if (1 == ret) {
    map<string, CWX_UINT8> topics = m_config.getTopics();
    map<string, CWX_UINT8>* changedTopics = new map<string, CWX_UINT8>;
    if (0 != m_pTopicMgr->updateTopicBinlogMgr(topics,
        *changedTopics,
        m_config.getBinLog().m_uiBinLogMSize,
        m_config.getBinLog().m_uiFlushNum,
        m_config.getBinLog().m_uiFlushSecond,
        m_config.getBinLog().m_uiMgrFileNum,
        m_config.getBinLog().m_uiSaveFileDay,
        true,
        m_szErr2K)){
      CWX_ERROR(("Failure to pdate topic binlogMgr, err:%s", m_szErr2K));
      return ;
    }
    if (changedTopics->size()){ ///发送新增topic给内部分发线程
      CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(sizeof(changedTopics));
      memcpy(msg->wr_ptr(), &changedTopics, sizeof(changedTopics));
      msg->wr_ptr(sizeof(changedTopics));
      msg->event().setSvrId(CwxMqApp::SVR_TYPE_INNER_DISP);
      msg->event().setEvent(EVENT_ZK_TOPIC_CHANGE);
      m_innerDispatchThreadPool->append(msg);
    }else {
      delete changedTopics;
    }
  }

}

///信号处理函数
void CwxMqApp::onSignal(int signum) {
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

int CwxMqApp::onConnCreated(CWX_UINT32 uiSvrId,
                            CWX_UINT32 uiHostId,
                            CWX_HANDLE handle,
                            bool&)
{
  if ((SVR_TYPE_INNER_DISP == uiSvrId) ||
      (SVR_TYPE_OUTER_DISP == uiSvrId)) {
    CwxMsgBlock* msg = CwxMsgBlockAlloc::malloc(0);
    msg->event().setSvrId(uiSvrId);
    msg->event().setHostId(uiHostId);
    msg->event().setConnId(CWX_APP_INVALID_CONN_ID);
    msg->event().setIoHandle(handle);
    msg->event().setEvent(CwxEventInfo::CONN_CREATED);
    if (SVR_TYPE_INNER_DISP == uiSvrId) { ///内部同步
      if (m_innerDispatchThreadPool->append(msg) <=1) m_innerDispatchChannel->notice();
    }else if (SVR_TYPE_OUTER_DISP == uiSvrId) { ///外部同步
      if (m_outerDispatchThreadPool->append(msg) <=1) m_outerDispatchChannel->notice();
    }
  }else {
    CWX_ASSERT(SVR_TYPE_MONITOR == uiSvrId);
  }
  return 0;
}

///连接建立
int CwxMqApp::onConnCreated(CwxAppHandler4Msg& conn,
                            bool&,
                            bool&)
{
  if (SVR_TYPE_RECV == conn.getConnInfo().getSvrId())
  { ///忽略处理
  } else if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId()) { ///如果是监控的连接建立，则建立一个string的buf，用于缓存不完整的命令
    string* buf = new string();
    conn.getConnInfo().setUserData(buf);
  } else {
    CWX_ASSERT(0);
  }
  return 0;
}

///连接关闭
int CwxMqApp::onConnClosed(CwxAppHandler4Msg& conn) {
  if (SVR_TYPE_MASTER == conn.getConnInfo().getSvrId()) {///如果是master的同步连接关闭
    CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(0);
    pBlock->event().setSvrId(conn.getConnInfo().getSvrId());
    pBlock->event().setHostId(conn.getConnInfo().getHostId());
    pBlock->event().setConnId(conn.getConnInfo().getConnId());
    ///设置事件类型
    pBlock->event().setEvent(CwxEventInfo::CONN_CLOSED);
    m_recvThreadPool->append(pBlock);
  } else if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId()) { ///若是监控的连接关闭，则必须释放先前所创建的string对象。
    if (conn.getConnInfo().getUserData()) {
      delete (string*) conn.getConnInfo().getUserData();
      conn.getConnInfo().setUserData(NULL);
    }
  }else{
    CWX_ASSERT(SVR_TYPE_RECV == conn.getConnInfo().getSvrId());
  }
  return 0;
}

///收到消息
int CwxMqApp::onRecvMsg(CwxMsgBlock* msg,
                        CwxAppHandler4Msg& conn,
                        CwxMsgHead const& header,
                        bool&)
{
  if (SVR_TYPE_RECV == conn.getConnInfo().getSvrId() ||
      SVR_TYPE_MASTER == conn.getConnInfo().getSvrId())
  {
    msg->event().setSvrId(conn.getConnInfo().getSvrId());
    msg->event().setHostId(conn.getConnInfo().getHostId());
    msg->event().setConnId(conn.getConnInfo().getConnId());
    ///保存消息头
    msg->event().setMsgHeader(header);
    ///设置事件类型
    msg->event().setEvent(CwxEventInfo::RECV_MSG);
    ///将消息放到线程池队列中，有内部的线程调用其处理handle来处理
    m_recvThreadPool->append(msg);
    return 0;
  } else {
    CWX_ASSERT(0);
  }
  return 0;
}

///收到消息的响应函数
int CwxMqApp::onRecvMsg(CwxAppHandler4Msg& conn, bool&) {
  if (SVR_TYPE_MONITOR == conn.getConnInfo().getSvrId()) {
    char szBuf[1024];
    ssize_t recv_size = CwxSocket::recv(conn.getHandle(), szBuf, 1024);
    if (recv_size <= 0) { //error or signal
      if ((0 == recv_size) || ((errno != EWOULDBLOCK) && (errno != EINTR))) {
        return -1; //error
      } else { //signal or no data
        return 0;
      }
    }
    ///监控消息
    return monitorStats(szBuf, (CWX_UINT32) recv_size, conn);
  } else {
    CWX_ASSERT(0);
  }
  return -1;
}

void CwxMqApp::saveMaxSid() {
  string strNewFileName = m_strMaxSidFile + ".new";
  int fd = ::open(strNewFileName.c_str(),  O_RDWR | O_CREAT | O_TRUNC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (-1 == fd) {
    CWX_ERROR(("Failure to open new max-sid file:%s, errno=%d",
        strNewFileName.c_str(), errno));
    ::close(fd);
    return ;
  }
  //写入topic
  char line[32];
  CwxCommon::toString(m_uiCurSid, line, 10);
  ssize_t len = strlen(line);
  ///[topic]
  if (len != write(fd, line, len)) {
    ::close(fd);
    CWX_ERROR(("Failure to write new max-sid file:%s, errno=%d", strNewFileName.c_str(), errno));
    return ;
  }
  ::close(fd);
  CwxFile::moveFile(strNewFileName.c_str(), m_strMaxSidFile.c_str());
  return ;
}

void CwxMqApp::destroy() {
  if (CwxMqConfigCmn::MQ_TYPE_ZK == m_config.getCommon().m_type) saveMaxSid();

  ///停止zk线程，必须限于zkHandler->stop,否则会死锁。
  if (m_zkThreadPool) m_zkThreadPool->stop();
  ///停止zookeeper底层线程
  if (m_zkHandler) {
    m_zkHandler->stop();
  }
  ///停止recv线程
  if (m_recvThreadPool) {
    m_recvThreadPool->stop();
  }
  ///停止内部发分发线程
  if (m_innerDispatchThreadPool) m_innerDispatchThreadPool->stop();
  ///停止外部分发线程
  if (m_outerDispatchThreadPool) m_outerDispatchThreadPool->stop();

  ///释放handler
  if (m_masterHandler) delete m_masterHandler;
  m_masterHandler = NULL;

  if (m_recvHandler) delete m_recvHandler;
  m_recvHandler = NULL;

  if (m_zkHandler) delete m_zkHandler;
  m_zkHandler = NULL;

  ///删除外部同步线程及channel
  if (m_outerDispatchThreadPool) delete m_outerDispatchThreadPool;
  m_outerDispatchThreadPool = NULL;
  if (m_outerDispatchChannel) delete m_outerDispatchChannel;
  m_outerDispatchChannel = NULL;

  ///删除内部同步线程及channel
  if (m_innerDispatchThreadPool) delete m_innerDispatchThreadPool;
  m_innerDispatchThreadPool = NULL;
  if (m_innerDispatchChannel) delete m_innerDispatchChannel;
  m_innerDispatchChannel = NULL;

  ///删除recv线程池
  if (m_recvThreadPool) delete m_recvThreadPool;
  m_recvThreadPool = NULL;

  ///删除zk线程池
  if (m_zkThreadPool) delete m_zkThreadPool;
  m_zkThreadPool = NULL;

  ///删除binlog管理器
  if (m_pTopicMgr) delete m_pTopicMgr;
  m_pTopicMgr = NULL;

  CwxAppFramework::destroy();
}

int CwxMqApp::monitorStats(char const* buf,
      CWX_UINT32 uiDataLen,
      CwxAppHandler4Msg& conn) {
  string* strCmd = (string*)conn.getConnInfo().getUserData();
  strCmd->append(buf, uiDataLen);
  CwxMsgBlock* msg = NULL;
  string::size_type end = 0;
  do{
    CwxCommon::trim(*strCmd);
    end = strCmd->find('\n');
    if (string::npos == end) {
      if (strCmd->length() > 10) { ///无效的命令
        strCmd->erase(); ///清空接收到的命令
        ///回复信息
        msg = CwxMsgBlockAlloc::malloc(1024);
        strcpy(msg->wr_ptr(), "ERROR\r\n");
      }else {
        return 0;
      }
    } else {
      if (memcmp(strCmd->c_str(), "stats", 5) == 0) {
        strCmd->erase(); ///清空接收到的命令
        CWX_UINT32 uiLen = packMonitorInfo();
        msg = CwxMsgBlockAlloc::malloc(uiLen);
        memcpy(msg->wr_ptr(), m_szBuf, uiLen);
        msg->wr_ptr(uiLen);
      } else if (memcmp(strCmd->c_str(), "quit", 4) == 0) {
        return -1;
      }else { ///无效的命令
        strCmd->erase(); ///清空收到的命令
        ///回复信息
        msg = CwxMsgBlockAlloc::malloc(1024);
        strcpy(msg->wr_ptr(), "ERROR\r\n");
        msg->wr_ptr(strlen(msg->wr_ptr()));
      }
    }
  }while(0);
  msg->send_ctrl().setConnId(conn.getConnInfo().getConnId());
  msg->send_ctrl().setSvrId(CwxMqApp::SVR_TYPE_MONITOR);
  msg->send_ctrl().setHostId(0);
  msg->send_ctrl().setMsgAttr(CwxMsgSendCtrl::NONE);
  if (-1 == sendMsgByConn(msg)) {
    CWX_ERROR(("Failure to send monitor reply"));
    CwxMsgBlockAlloc::free(msg);
    return -1;
  }
  return 0;
}

#define MQ_MONITOR_APPEND(){\
  uiLen = strlen(szLine);\
  if (uiPos + uiLen > MAX_MONITOR_REPLY_SIZE - 20) break;\
  memcpy(m_szBuf + uiPos, szLine, uiLen);\
  uiPos += uiLen; }

CWX_UINT32 CwxMqApp::packMonitorInfo() {
  string strValue;
  char szLine[4096];
  CWX_UINT32 uiLen = 0;
  CWX_UINT32 uiPos = 0;
  do {
    //输出进程pid
    CwxCommon::snprintf(szLine, 4096, "STAT pid %d\r\n", getpid());
    MQ_MONITOR_APPEND();
    //输出父进程pid
    CwxCommon::snprintf(szLine, 4096, "STAT ppid %d\r\n", getppid());
    MQ_MONITOR_APPEND();
    //版本号
    CwxCommon::snprintf(szLine, 4096, "STAT version %s\r\n",
      this->getAppVersion().c_str());
    MQ_MONITOR_APPEND();
    //修改时间
    CwxCommon::snprintf(szLine, 4096, "STAT modify %s\r\n",
      this->getLastModifyDatetime().c_str());
    MQ_MONITOR_APPEND();
    //编译时间
    CwxCommon::snprintf(szLine, 4096, "STAT compile %s\r\n",
      this->getLastCompileDatetime().c_str());
    MQ_MONITOR_APPEND();
    //启动时间
    CwxCommon::snprintf(szLine, 4096, "STAT start %s\r\n",
      m_strStartTime.c_str());
    MQ_MONITOR_APPEND();
    //server_type
    CwxCommon::snprintf(szLine, 4096, "STAT server_type ");
    if (m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_MASTER) {
      CwxCommon::snprintf(szLine, 4096, "master\r\n");
    }else if (m_config.getCommon().m_type == CwxMqConfigCmn::MQ_TYPE_SLAVE) {
      CwxCommon::snprintf(szLine, 4096, "slave\r\n");
    }else {
      CwxCommon::snprintf(szLine, 4096, "zk\r\n");
    }
    MQ_MONITOR_APPEND();
    //binlog
    list<CwxMqZkTopicSid> lst;
    m_pTopicMgr->dumpTopicInfo(lst);
    CwxCommon::snprintf(szLine, 4096, "Topic size:%u\r\n", lst.size());
    MQ_MONITOR_APPEND();
    char szSid1[64];
    char szSid2[64];
    for(list<CwxMqZkTopicSid>::iterator iter = lst.begin(); iter != lst.end(); iter++) {
      CwxCommon::snprintf(szLine, 4096,"Topic:%s, state:%d, start_sid:%s, max_sid:%s\r\n",
          iter->m_strTopic.c_str(), iter->m_state,
          CwxCommon::toString(iter->m_ullMinSid, szSid1, 10),
          CwxCommon::toString(iter->m_ullSid, szSid2, 10));
      MQ_MONITOR_APPEND();
    }
  } while (0);
  strcpy(m_szBuf + uiPos, "END\r\n");
  return strlen(m_szBuf);

}
