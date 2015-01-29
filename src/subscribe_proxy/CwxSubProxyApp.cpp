#include "CwxSubProxyApp.h"
#include "CwxDate.h"

///构造函数
CwxSubProxyApp::CwxSubProxyApp() {
  m_zkHandler = NULL;
  m_zkThreadPool = NULL;
  m_newMqGroup = NULL;
  m_ttCurTime = 0;
}

///析构函数
CwxSubProxyApp::~CwxSubProxyApp() {
}

///初始化
int CwxSubProxyApp::init(int argc, char** argv) {
  string strErrMsg;
  ///首先调用架构的init api
  if (CwxAppFramework::init(argc, argv) == -1) return -1;
  ///检查是否通过-f指定了配置文件，若没有，则采用默认的配置文件
  if ((NULL == this->getConfFile()) || (strlen(this->getConfFile()) == 0)) {
    this->setConfFile("subscribe_proxy.conf");
  }
  ///加载配置文件，若失败则退出
  if (0 != m_config.loadConfig(getConfFile())) {
    CWX_ERROR((m_config.getErrMsg()));
    return -1;
  }

  ///设置运行日志的输出level
  setLogLevel(CwxLogger::LEVEL_ERROR | CwxLogger::LEVEL_INFO
    | CwxLogger::LEVEL_WARNING);
  return 0;
}

///配置环境信息
int CwxSubProxyApp::initRunEnv() {
  ///设置系统的时钟间隔，最小刻度为1ms，此为0.1s。
  this->setClick(100); //0.1s
  ///设置工作目录
  this->setWorkDir(m_config.getCommon().m_strWorkDir.c_str());
  ///设置循环运行日志的数量
  this->setLogFileNum(LOG_FILE_NUM);
  ///设置每个日志文件的大小
  this->setLogFileSize(LOG_FILE_SIZE * 1024 * 1024);
  ///调用架构的initRunEnv，使以上设置的参数生效
  if (CwxAppFramework::initRunEnv() == -1) return -1;
  ///将加载的配置文件信息输出到日志文件中，以供查看检查
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
  this->setAppVersion(CWX_SUB_PROXY_VERSION);
  //set last modify date
  this->setLastModifyDatetime(CWX_SUB_PROXY_MODIFY);
  //set compile date
  this->setLastCompileDatetime(CWX_COMPILE_DATE(_BUILD_DATE));
  ///设置启动时间
  CwxDate::getDateY4MDHMS2(time(NULL), m_strStartTime);

  m_tss = new CwxMqTss();
  m_tss->init();
  ///启动zk线程池
  m_zkThreadPool = new CwxThreadPool(1,
      &getCommander(),
      CwxSubProxyApp::zkThreadMain,
      this);
  CwxTss** pTss = new CwxTss*[1];
  pTss[0] = new CwxMqTss();
  ((CwxMqTss*)pTss[0])->init();
  if (0 != m_zkThreadPool->start(pTss)) {
    CWX_ERROR(("Failure to start zookeeper thread pool."));
    return -1;
  }
  ///注册zk的handler
  m_zkHandler = new CwxSubProxyZkHandler(this, m_config.getZk().m_strRootPath,
      m_config.getZk().m_strZkServer);
  if (0 != m_zkHandler->init()) {
    CWX_ERROR(("Failure to init zk handler."));
    return -1;
  }

  ///启动监听端口
  if (0 != startNetwork()) return -1;
  return 0;
}

///zk消息处理handler
void* CwxSubProxyApp::zkThreadMain(CwxTss* tss, CwxMsgQueue* queue, void* arg) {
  CwxSubProxyApp* app = (CwxSubProxyApp*)arg;
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
void CwxSubProxyApp::onTime(CwxTimeValue const& current) {
  ///调用基类的onTime函数
  CwxAppFramework::onTime(current);
  m_ttCurTime = current.sec();
  ///检查超时
  static CWX_UINT32 ttTimeBase = 0; ///<时钟回跳的base时钟
  static CWX_UINT32 ttLastTime = m_ttCurTime; ///<上一次检查的时间
  bool bClockBack = isClockBack(ttTimeBase, m_ttCurTime);
  if (bClockBack || (m_ttCurTime >= ttLastTime + 1)) {
    CwxMsgBlock* pBlock = NULL;
    ttLastTime = m_ttCurTime;
    // 往zk线程池append TIMEOUT_CHECK
    if (m_zkThreadPool) {
      pBlock = CwxMsgBlockAlloc::malloc(0);
      pBlock->event().setSvrId(SVR_SUB_TYPE_SYNC);
      pBlock->event().setEvent(CwxEventInfo::TIMEOUT_CHECK);
      //将超时检查事件，放入事件队列
      m_zkThreadPool->append(pBlock);
    }
    // 检查sync host是否改变
    checkSyncHostModify();
  }
}

///信号处理函数
void CwxSubProxyApp::onSignal(int signum) {
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

void CwxSubProxyApp::destroy() {
  // 释放消息获取的线程池及channel
  if (m_zkThreadPool) {
    m_zkThreadPool->stop();
    delete m_zkThreadPool;
    m_zkThreadPool = NULL;
  }
  if (m_zkHandler) {
    delete m_zkHandler;
    m_zkHandler = NULL;
  }
  // 释放数据同步的线程池及channel
  map<CWX_INT32, CwxSubSyncSession*>::iterator iter = m_syncs.begin();
  while (iter != m_syncs.end()) {
    stopSync(iter->first);
    iter = m_syncs.begin();
  }
  if (m_newMqGroup){
    delete m_newMqGroup;
    m_newMqGroup = NULL;
  }
  CwxAppFramework::destroy();
}

int CwxSubProxyApp::startNetwork() {
  ///打开同步监听端口
  if (0 > this->noticeTcpListen(SVR_SUB_TYPE_RECV,
      m_config.getCommon().m_listen.getHostName().c_str(),
      m_config.getCommon().m_listen.getPort(),
      false,
      CWX_APP_MSG_MODE,
      CwxSubProxyApp::setRecvSockAttr,
      this)) {
    CWX_ERROR(("Can't register the recv tcp accept listen: addr=%s, port=%d",
        m_config.getCommon().m_listen.getHostName().c_str(),
        m_config.getCommon().m_listen.getPort()));
    return -1;
  }
  return 0;
}

///连接建立
int CwxSubProxyApp::onConnCreated(CwxAppHandler4Msg& , bool&, bool&) {
  return 0;
}

///连接关闭
int CwxSubProxyApp::onConnClosed(CwxAppHandler4Msg& conn) {
  stopSync(conn.getConnInfo().getConnId());
  return 0;
}

///收到消息
int CwxSubProxyApp::onRecvMsg(CwxMsgBlock* msg,
    CwxAppHandler4Msg& conn,
    CwxMsgHead const& header,
    bool&){
  if (SVR_SUB_TYPE_RECV == conn.getConnInfo().getSvrId()) {
    msg->event().setSvrId(conn.getConnInfo().getSvrId());
    msg->event().setHostId(conn.getConnInfo().getHostId());
    msg->event().setConnId(conn.getConnInfo().getConnId());
    ///保存消息头
    msg->event().setMsgHeader(header);
    return recvMsg(msg, conn);
  }else {
    CWX_ERROR(("Recv unkown svr type:%d", conn.getConnInfo().getSvrId()));
    CWX_ASSERT(0);
  }
  return 0;
}

int CwxSubProxyApp::recvMsg(CwxMsgBlock* msg, CwxAppHandler4Msg& conn) {
  int iRet;
  char szAddr[64];
  conn.getRemoteAddr(szAddr, sizeof(szAddr));
  ///CWX_INFO(("Recv msg:%d from host:%s:%u", msg->event().getMsgHeader().getMsgType(),
      ////szAddr, conn.getRemotePort()));
  do {
    if (CwxMqPoco::MSG_TYPE_PROXY_REPORT == msg->event().getMsgHeader().getMsgType()) {
      if (m_syncs.find(msg->event().getConnId()) != m_syncs.end()) {
        ///重复发送report
        CwxCommon::snprintf(m_tss->m_szBuf2K, 2047, "Recv duplicate report.");
        CWX_ERROR(("%s, from:%s:%d", m_tss->m_szBuf2K, szAddr, conn.getRemotePort()));
        break;
      }
      ///同步点报告
      CWX_UINT64 ullSid=0;
      char const* szGroup;
      char const* szTopic;
      char const* szSource;
      bool zip;
      bool bNewly;
      CWX_UINT32 uiChunkSize;
      iRet = CwxMqPoco::parseProxyReportData(m_tss->m_pReader,
          msg,
          ullSid,
          szGroup,
          szTopic,
          bNewly,
          uiChunkSize,
          szSource,
          zip,
          m_tss->m_szBuf2K);
      if (CWX_MQ_ERR_SUCCESS != iRet) {
        CWX_ERROR(("Failure to parse report msg, err=%s from:%s:%d", szAddr, conn.getRemotePort()));
        break;
      }
      ///启动同步线程
      if (0 != startSync(szGroup,
          szTopic,
          szSource,
          ullSid,
          uiChunkSize,
          zip,
          bNewly,
          msg->event().getConnId())) {
        iRet = CWX_MQ_ERR_ERROR;
        break;
      }
      CWX_INFO(("Success start sync session group:%s topic:%s, source:%s, from %s:%u",
          szGroup, szTopic, szSource, szAddr, conn.getRemotePort()));
      return 0;
    }else if (CwxMqPoco::MSG_TYPE_PROXY_SYNC_DATA_REPLY == msg->event().getMsgHeader().getMsgType() ||
        CwxMqPoco::MSG_TYPE_PROXY_SYNC_CHUNK_DATA_REPLY == msg->event().getMsgHeader().getMsgType()) {
      map<CWX_INT32, CwxSubSyncSession*>::iterator iter = m_syncs.find(msg->event().getConnId());
      if (iter == m_syncs.end()) {
        CwxCommon::snprintf(m_tss->m_szBuf2K, 2047, "Should send report first.");
        CWX_ERROR(("%s from:%s:%u", m_tss->m_szBuf2K, szAddr, conn.getRemotePort()));
        break;
      }
      msg->event().setEvent(CwxEventInfo::RECV_MSG);
      iter->second->m_threadPool->append(msg);
      return 0;
    }else {
      ///未知消息类型，关闭该conn_id
      CWX_ERROR(("Unkown msg type:%d from conn_id:%d", msg->event().getMsgHeader().getMsgType(),
          msg->event().getConnId()));
      return 0;
    }
  }while(0);
  ///到此一定错误
  CwxMsgBlock* pBlock = NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncErr(m_tss->m_pWriter,
      pBlock,
      msg->event().getMsgHeader().getTaskId(),
      CWX_MQ_ERR_ERROR,
      m_tss->m_szBuf2K,
      m_tss->m_szBuf2K)) {
    CWX_ERROR(("Failure to packSyncErr err:%s", m_tss->m_szBuf2K));
    return -1;
  }
  pBlock->send_ctrl().setMsgAttr(CwxMsgSendCtrl::CLOSE_NOTICE);
  pBlock->send_ctrl().setConnId(msg->event().getConnId());
  pBlock->send_ctrl().setSvrId(msg->event().getSvrId());
  pBlock->send_ctrl().setHostId(msg->event().getHostId());
  if (0 != this->sendMsgByConn(pBlock)) {
    CWX_ERROR(("Failure to send msg, from:%s:%u", szAddr, conn.getRemotePort()));
    return -1;
  }
  return 0;
}
/// 停止sync。返回值，0：成功；-1：失败
int CwxSubProxyApp::stopSync(CWX_INT32 uiConnId){
  this->noticeCloseConn(uiConnId);
  map<CWX_INT32, CwxSubSyncSession*>::iterator iter = m_syncs.find(uiConnId);
  if (iter == m_syncs.end()) return 0;
  // 将host从map中删除
  m_syncs.erase(iter);
  CwxSubSyncSession* pSession = iter->second;

  // 停止线程
  if (pSession->m_threadPool){
    pSession->m_threadPool->stop();
    delete pSession->m_threadPool;
    pSession->m_threadPool = NULL;
  }
  // 关闭channel
  if (pSession->m_channel){
    delete pSession->m_channel;
    pSession->m_channel = NULL;
  }
  // 删除session对象
  delete pSession;
  return 0;
}

/// 启动sync。返回值，0：成功；-1：失败
int CwxSubProxyApp::startSync(char const* szGroup,
    char const* szTopic,
    char const* szSource,
    CWX_UINT64 ullSid,
    CWX_UINT32 uiChunkSize,
    bool bZip,
    bool bNewly,
    CWX_INT32 uiConnId){
  map<string, CwxZkMqGroup>::iterator mp_iter = m_group.find(szGroup);
  if (mp_iter == m_group.end()){
    CwxCommon::snprintf(m_tss->m_szBuf2K, 2047, "Failure to get q-group[%s] info.", szGroup);
    CWX_ERROR(("%s", m_tss->m_szBuf2K));
    return -1;
  }
  set<CWX_UINT32> hostIds;
  map<CWX_INT32, CwxSubSyncSession*>::iterator iter = m_syncs.begin();
  while(iter != m_syncs.end()){
    hostIds.insert(iter->second->m_uiHostId);
    ++iter;
  }
  // 选取host id
  while(true){
    m_uiCurHostId++;
    if (!m_uiCurHostId) m_uiCurHostId = 1;
    if (hostIds.find(m_uiCurHostId) == hostIds.end()) break;
  }

  CwxSubSyncSession* pSession = NULL;
  pSession = new CwxSubSyncSession();
  m_syncs[uiConnId] = pSession;
  pSession->m_syncHost = mp_iter->second.m_masterHost;
  pSession->m_uiHostId = m_uiCurHostId;
  pSession->m_bClosed = true;
  pSession->m_bNeedClosed = false;
  pSession->m_pApp = this;
  pSession->m_strGroup = szGroup;
  pSession->m_strTopic = szTopic;
  pSession->m_strSource = szSource;
  pSession->m_uiReplyConnId = uiConnId;
  pSession->m_ullLogSid = ullSid;
  pSession->m_uiChunkSize = uiChunkSize;
  pSession->m_bZip = bZip;
  pSession->m_bNewly = bNewly;
  CWX_INFO(("Group:%s, topic:%s, source:%s, zip:%d, chunk:%u master[%s:%d] start session", szGroup, szTopic, szSource,
      bZip, uiChunkSize,
      pSession->m_syncHost.getHostName().c_str(),
      pSession->m_syncHost.getPort()));
  pSession->m_channel = new CwxAppChannel();
  //创建线程池
  pSession->m_threadPool = new CwxThreadPool(1,
    &getCommander(),
    CwxSubProxyApp::syncThreadMain,
    pSession);
  ///启动线程
  CwxTss** pTss = new CwxTss*[1];
  pTss[0] = new CwxMqTss();
  ((CwxMqTss*) pTss[0])->init();
  if (0 != pSession->m_threadPool->start(pTss)) {
    CwxCommon::snprintf(m_tss->m_szBuf2K, 2047, "Failure to start sync thread pool, errno=%d", errno);
    CWX_ERROR(("%s", m_tss->m_szBuf2K));
    return -1;
  }
  return 0;
}

/// 更新sync。返回值，0：成功；-1：失败
int CwxSubProxyApp::noticeSyncHost(string group, CwxHostInfo const& hostInfo){
  map<CWX_INT32, CwxSubSyncSession*>::iterator iter = m_syncs.begin();
  while (iter != m_syncs.end()){
    if (iter->second->m_strGroup == group) {
      CwxHostInfo* host = new CwxHostInfo(hostInfo);
      CwxMsgBlock* pBlock = CwxMsgBlockAlloc::malloc(sizeof(CwxHostInfo*));
      memcpy(pBlock->wr_ptr(), &host, sizeof(host));
      pBlock->wr_ptr(sizeof(host));
      pBlock->event().setSvrId(SVR_SUB_TYPE_SYNC);
      pBlock->event().setEvent(EVENT_TYPE_SYNC_CHANGE);
      iter->second->m_threadPool->append(pBlock);
      CWX_INFO(("Notice group:%s, host[%s:%d]", group.c_str(), hostInfo.getHostName().c_str(),
          hostInfo.getPort()));
    }
    iter++;
  }
  return 0;
}

/// 检查是否sync host发生了改变
void CwxSubProxyApp::checkSyncHostModify(){
  map<string, CwxZkMqGroup>*   nowGroup = NULL;
  {
    CwxMutexGuard<CwxMutexLock> lock(&m_mqMutex);
    if (!m_newMqGroup) return;
    nowGroup = m_newMqGroup;
    m_newMqGroup = NULL;
  }
  map<string, CwxZkMqGroup>::iterator group_iter;
  map<string, CwxZkMqGroup>::iterator iter = nowGroup->begin();
  while(iter != nowGroup->end()){
    group_iter = m_group.find(iter->first);
    if (group_iter == m_group.end()) { // 新加的host
      m_group[iter->first] = iter->second;
    }else{//检查是否改变
      if ((iter->second.m_masterHost.getPort() != group_iter->second.m_masterHost.getPort()) ||
        (iter->second.m_masterHost.getHostName() != group_iter->second.m_masterHost.getHostName()))
      {
        m_group[iter->first] = iter->second;
        if (0 != noticeSyncHost(iter->first, iter->second.m_masterHost)) {
          CWX_ERROR(("Failure to update sync[%s], exit.", iter->first.c_str()));
          this->stop();
          return;
        }
      }
    }
    ++iter;
  }
  // 检查是否有被删除的group
  group_iter = m_group.begin();
  while(group_iter != m_group.end()){
    if (nowGroup->find(group_iter->first) == nowGroup->end()){
      {///检测同步点，删除在同步的group
        for(map<CWX_INT32, CwxSubSyncSession*>::iterator tmp_iter = m_syncs.begin();
            tmp_iter != m_syncs.end();) {
          CWX_INT32 connId = tmp_iter->first;
          if (tmp_iter->second->m_strGroup == group_iter->first) {
            CwxCommon::snprintf(m_tss->m_szBuf2K, 2047, "Mq-group is deleted.");
            replyError(connId);
            stopSync(connId);
          }
          tmp_iter = m_syncs.upper_bound(connId);
        }

      }
      m_group.erase(group_iter++);
      continue;
    }
    ++group_iter;
  }
  delete nowGroup;
}

void CwxSubProxyApp::replyError(CWX_INT32 uiConnId) {
  CwxMsgBlock* block = NULL;
  if (CWX_MQ_ERR_SUCCESS != CwxMqPoco::packSyncErr(m_tss->m_pWriter, block, 0,
      CWX_MQ_ERR_ERROR, m_tss->m_szBuf2K, m_tss->m_szBuf2K)) {
     CWX_ERROR(("Failure to pack sync err:%s", m_tss->m_szBuf2K));
     return ;
  }
  block->send_ctrl().setConnId(uiConnId);
  block->send_ctrl().setSvrId(CwxSubProxyApp::SVR_SUB_TYPE_RECV);
  block->send_ctrl().setHostId(0);
  this->sendMsgByConn(block);
}

///sync channel的线程函数，arg为app对象
void* CwxSubProxyApp::syncThreadMain(CwxTss* tss,
                               CwxMsgQueue* queue,
                               void* arg)
{
  CwxMqTss* pTss = (CwxMqTss*)tss;
  CwxSubSyncSession* pSession = (CwxSubSyncSession*) arg;
  pTss->m_userData = arg;
  if (0 != pSession->m_channel->open()) {
    CWX_ERROR(("Failure to open sync channel"));
    //停止app
    pSession->m_pApp->stop();
    return NULL;
  }
  while (1) {
    //获取队列中的消息并处理
    if (0 != dealSyncThreadMsg(queue, pSession, pSession->m_channel)) break;
    if (-1 == pSession->m_channel->dispatch(2)) {
      CWX_ERROR(("Failure to invoke CwxAppChannel::dispatch()"));
      sleep(1);
    }
    if (pSession->isCloseSession()){
      CwxSubSyncHandler::closeSession(pTss);
    }else if (pSession->isNeedCreate()){
      CwxSubSyncHandler::createSession(pTss);
    }else if (pSession->isTimeout(pSession->m_pApp->getCurTime())){
      CwxSubSyncHandler::closeSession(pTss);
    }
  }
  pSession->m_channel->stop();
  pSession->m_channel->close();
  return NULL;
}
///sync channel的队列消息函数。返回值：0：正常；-1：队列停止
int CwxSubProxyApp::dealSyncThreadMsg(CwxMsgQueue* queue,
    CwxSubSyncSession* pSession,
    CwxAppChannel* )
{
  int iRet = 0;
  CwxMsgBlock* block = NULL;
  ///CwxSubSyncHandler* handler = (CwxSubSyncHandler*)(pSession->m_channel);
  while (!queue->isEmpty()) {
    do {
      iRet = queue->dequeue(block);
      if (-1 == iRet) return -1;
      if (block->event().getEvent() == EVENT_TYPE_SYNC_CHANGE){
        CwxHostInfo* host = NULL;
        memcpy(&host, block->rd_ptr(), sizeof(&host));
        pSession->m_syncHost = *host;
        pSession->m_bNeedClosed = true;
      }else if (CwxMqPoco::MSG_TYPE_PROXY_SYNC_CHUNK_DATA_REPLY == block->event().getMsgHeader().getMsgType()){
        if (-1 == CwxSubSyncHandler::dealSyncDataReply(block, pSession)) pSession->setNeedStop();
      }else if (CwxMqPoco::MSG_TYPE_PROXY_SYNC_DATA_REPLY == block->event().getMsgHeader().getMsgType()) {
        if (-1 == CwxSubSyncHandler::dealSyncChunkDataRely(block, pSession)) pSession->setNeedStop();
      }else {
        CWX_ERROR(("Unknown event[%d] type[%d] for sync thread pool.", block->event().getEvent(),
            block->event().getMsgHeader().getMsgType()));
      }
    } while (0);
    if(block) CwxMsgBlockAlloc::free(block);
    block = NULL;
  }
  if (queue->isDeactived()) return -1;
  return 0;
}

int CwxSubProxyApp::setRecvSockAttr(CWX_HANDLE handle, void* arg) {
  CwxSubProxyApp* app = (CwxSubProxyApp*) arg;
  if (0 != CwxSocket::setKeepalive(handle,
    true,
    CWX_APP_DEF_KEEPALIVE_IDLE,
    CWX_APP_DEF_KEEPALIVE_INTERNAL,
    CWX_APP_DEF_KEEPALIVE_COUNT))
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u to keep-alive, errno=%d",
      app->getConfig().getCommon().m_listen.getHostName().c_str(),
      app->getConfig().getCommon().m_listen.getPort(),
      errno));
    return -1;
  }


  int flags = 1;
  if (setsockopt(handle,
    IPPROTO_TCP, TCP_NODELAY,
    (void *) &flags,
    sizeof(flags)) != 0)
  {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u NODELAY, errno=%d",
        app->getConfig().getCommon().m_listen.getHostName().c_str(),
        app->getConfig().getCommon().m_listen.getPort(), errno));
    return -1;
  }
  struct linger ling = { 0, 0 };
  if (setsockopt(handle, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling)) != 0) {
    CWX_ERROR(("Failure to set listen addr:%s, port:%u LINGER, errno=%d",
        app->getConfig().getCommon().m_listen.getHostName().c_str(),
        app->getConfig().getCommon().m_listen.getPort(), errno));
    return -1;
  }
  return 0;
}
