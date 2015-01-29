#ifndef __CWX_SUB_SYNC_HANLDER_H__
#define __CWX_SUB_SYNC_HANLDER_H__

#include "CwxCommander.h"
#include "CwxMqMacro.h"
#include "CwxMsgBlock.h"
#include "CwxMqTss.h"
#include "CwxThreadPool.h"
#include "CwxAppChannel.h"
#include "CwxHostInfo.h"

class CwxSubProxyApp;
class CwxSubSyncHandler;
class CwxSubRecvHandler;

///binlog订阅代理的session
class CwxSubSyncSession {
public:
  ///构造函数
  CwxSubSyncSession() {
    m_ullSessionId = 0;
    m_ullNextSeq = 0;
    m_ullLogSid = 0;
    m_uiLogTimeStamp = 0;
    m_uiHostId = 0;
    m_bNeedClosed = false;
    m_bClosed = true;
    m_bZip = false;
    m_bNeedStop = false;
    m_bNewly = false;
    m_uiLastConnectTimestamp = 0;
    m_unThreadPoolId = 0;
    m_uiReplyConnId = 0;
    m_uiChunkSize = 0;
    m_threadPool = NULL;
    m_channel = NULL;
    m_pApp = NULL;
  }
  ///析构函数
  ~CwxSubSyncSession() {
    map<CWX_UINT64, CwxMsgBlock*>::iterator iter = m_msg.begin();
    while (iter != m_msg.end()) {
      CwxMsgBlockAlloc::free(iter->second);
      iter++;
    }
  }
public:
  ///收到新消息，返回已经收到的消息列表
  bool recv(CWX_UINT64 ullSeq,
      CwxMsgBlock* msg,
      list<CwxMsgBlock*>& finished) {
    finished.clear();
    if (ullSeq == m_ullNextSeq) {
      finished.push_back(msg);
      m_ullNextSeq++;
      map<CWX_UINT64/*seq*/, CwxMsgBlock*>::iterator iter = m_msg.begin();
      while (iter != m_msg.end()) {
        if (iter->first == m_ullNextSeq) {
          finished.push_back(iter->second);
          m_ullNextSeq++;
          m_msg.erase(iter);
          iter = m_msg.begin();
          continue;
        }
        break;
      }
      return true;
    }
    m_msg[ullSeq] = msg;
    msg->event().setTimestamp((CWX_UINT32) time(NULL));
    return true;
  }
  ///检测是否超时
  bool isTimeout(CWX_UINT32 uiTimeout) const {
    if (!m_msg.size()) return false;
    CWX_UINT32 uiNow = time(NULL);
    return m_msg.begin()->second->event().getTimestamp() + uiTimeout < uiNow;
  }
  ///检查是否需要关闭
  inline bool isCloseSession() const {
    return m_bNeedClosed;
  }
  ///检查是否需要重建session
  bool isNeedCreate() const {
    return m_bClosed ;///&& (m_uiLastConnectTimestamp + CWX_MQ_DEF_TIMEOUT_SECOND < (CWX_UINT32)time(NULL));
  }
  ///设置停止同步状态
  void setNeedStop() {
    m_bNeedStop = true;
  }
  ///检查是否需要stop
  bool isNeedStop() const {
    return m_bNeedStop;
  }
public:
  CWX_UINT64                           m_ullSessionId; ///<session id
  CWX_UINT64                           m_ullNextSeq; ///<下一个待接收的sid
  CWX_UINT32                           m_uiHostId; ///<主机的id
  map<CWX_UINT64, CwxMsgBlock*>        m_msg; ///<等待排序的消息
  map<CWX_UINT32, CwxSubSyncHandler*>  m_conns;///<建立的连接
  CwxSubRecvHandler*                   m_recvHandler; ///<client的消息handler
  CWX_UINT16                           m_unThreadPoolId; ///<session对应的连接池
  CwxThreadPool*                       m_threadPool;
  CwxAppChannel*                       m_channel; ///<session对应的channel
  CwxHostInfo                          m_syncHost; ///<数据同步的主机
  string                               m_syncHostId; ///<同步主机的id
  CWX_INT32                            m_uiReplyConnId; ///<订阅用户的conn id
  string                               m_strGroup; ///<订阅的组
  string                               m_strTopic; ///<订阅的topic
  string                               m_strSource; ///<用户的source
  CWX_UINT32                           m_uiChunkSize; ///<chunk的大小
  bool                                 m_bZip; ///<是否压缩
  bool                                 m_bNewly; ///<是否未指定sid
  CwxSubProxyApp*                      m_pApp; ///<app对象
  bool                                 m_bNeedStop; ///<是否有错误发生需要关闭同步
  volatile CWX_UINT64                  m_ullLogSid; ///<当前同步的sid
  volatile CWX_UINT32                  m_uiLogTimeStamp; ///<当前同步的时间点
  volatile bool                        m_bNeedClosed; ///<session是否需要关闭
  volatile bool                        m_bClosed; ///<session是否已经关闭
  volatile CWX_UINT32                  m_uiLastConnectTimestamp; ///<上次连接时间
};


///从mq同步和数据的处理handle
class CwxSubSyncHandler : public CwxAppHandler4Channel {
public:
  ///构造函数
  CwxSubSyncHandler(CwxMqTss* pTss, CwxAppChannel* channel, CWX_UINT32 uiConnID) : CwxAppHandler4Channel(channel){
    m_pTss = pTss;
    m_uiConnId = uiConnID;
    m_uiRecvHeadLen = 0;
    m_uiRecvDataLen = 0;
    m_recvMsgData = NULL;
  }
  ///析构函数
  virtual ~CwxSubSyncHandler() {
    if (m_recvMsgData) CwxMsgBlockAlloc::free(m_recvMsgData);
  }
public:
  /**
  @brief 连接可读事件，返回-1，close()会被调用
  @return -1：处理失败，会调用close()； 0：处理成功
  */
  virtual int onInput();
  /**
  @brief 通知连接关闭。
  @return 1：不从engine中移除注册；0：从engine中移除注册但不删除handler；-1：从engine中将handle移除并删除。
  */
  virtual int onConnClosed();
  /// 获取连接id
  CWX_UINT32 getConnId() const { return m_uiConnId;}
  //关闭已有连接
  static void closeSession(CwxMqTss* pTss);
  ///创建与mq同步的连接。返回值：0：成功；-1：失败
  static int createSession(CwxMqTss* pTss);
  ///处理返回的sync chunk data reply。返回值：0：成功；-1：失败
  static int dealSyncChunkDataRely(CwxMsgBlock*& msg, CwxSubSyncSession* pSession);
  ///处理收到的sync data reply。返回值：0：成功；-1：失败
  static int dealSyncDataReply(CwxMsgBlock*& msg, CwxSubSyncSession* pSession);
private:
  ///接收消息，0：成功；-1：失败
  int recvMessage();
  ///从session中接受消息；0：成功；-1：失败
  int recvMsg(CwxMsgBlock*& msg, list<CwxMsgBlock*>& msgs);
  ///处理Sync report的reply消息。返回值：0：成功；-1：失败
  int dealSyncReportReply(CwxMsgBlock*& msg);
  ///处理收到的sync data。返回值：0：成功；-1：失败
  int dealSyncData(CwxMsgBlock*& msg);
  //处理收到的chunk模式下的sync data。返回值：0：成功；-1：失败
  int dealSyncChunkData(CwxMsgBlock*& msg);
  //处理错误消息。返回值：0：成功；-1：失败
  int dealErrMsg(CwxMsgBlock*& msg);
  // 获取压缩的buf大小
  inline CWX_UINT32 getBufLen() const{
    return CWX_MQ_MAX_CHUNK_KSIZE  * 1024 + CWX_MQ_MAX_MSG_SIZE;
  }
private:
  CwxMsgHead             m_header; ///<消息头
  CwxMqTss*              m_pTss; ///<线程的tss对象
  CWX_UINT32             m_uiConnId;///<连接id
  CwxPackageReader       m_reader;///<数据包的捷报对象
  char                   m_szHeadBuf[CwxMsgHead::MSG_HEAD_LEN + 1]; ///<消息头buffer
  CWX_UINT32             m_uiRecvHeadLen; ///<接收到的消息头字节
  CWX_UINT32             m_uiRecvDataLen; ///<接收到的数据字节
  CwxMsgBlock*           m_recvMsgData; ///<接收到的消息
};

#endif
