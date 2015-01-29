#ifndef __CWX_MQ_DISP_HANDLER_H__
#define __CWX_MQ_DISP_HANDLER_H__

#include "CwxCommander.h"
#include "CwxAppAioWindow.h"
#include "CwxMqMacro.h"
#include "CwxMqTss.h"
#include "CwxMqDef.h"
#include "CwxAppHandler4Channel.h"
#include "CwxAppChannel.h"
#include "CwxSidLogFile.h"

class CwxMqApp;
class CwxMqOuterDispHandler;

///分发连接的sync session信息对象
class CwxMqOuterDispSession {
public:
  ///构造函数
  CwxMqOuterDispSession(CwxMqApp* app, CwxBinLogMgr* binlogMgr) {
    m_ullLeftBinlogNum = 0;
    m_ullSeq = 0;
    m_ullSessionId = 0;
    m_bClosed = false;
    m_pCursor = NULL;
    m_uiChunk = 0;
    m_ullStartSid = 0;
    m_ullSid = 0;
    m_bNext = false;
    m_bZip = false;
    m_sourceFile = NULL;
    m_pApp = app;
    m_binlogMgr = binlogMgr;
  }
  ~CwxMqOuterDispSession();
public:
  void addConn(CwxMqOuterDispHandler* conn);
  ///重新形成session id，返回session id
  CWX_UINT64 reformSessionId() {
    CwxTimeValue timer;
    timer.now();
    m_ullSessionId = timer.to_usec();
    return m_ullSessionId;
  }

public:
  CWX_UINT64                         m_ullSessionId; ///<session id
  CWX_UINT64                         m_ullSeq; ///<当前的序列号，从0开始。
  bool                               m_bClosed; ///<是否需要关闭
  map<CWX_UINT32, CwxMqOuterDispHandler*> m_conns; ///<建立的连接
  CwxBinLogCursor*                   m_pCursor; ///<binlog的读取cursor
  CWX_UINT32                         m_uiChunk; ///<chunk大小
  string                             m_strTopic; ///订阅的topic
  CWX_UINT64                         m_ullStartSid; ///<report的sid
  CWX_UINT64                         m_ullSid; ///<当前发送到的sid
  CWX_UINT64                         m_ullLeftBinlogNum; ///<binlog剩余的数量
  bool                               m_bNext; ///<是否发送下一个消息
  bool                               m_bZip; ///<是否压缩
  string                             m_strHost; ///<session的来源主机
  string                             m_strSource; ///<source的名字
  CwxSidLogFile*                     m_sourceFile; ///<source的文件
  CwxMqApp*                          m_pApp; ///<CwxMqApp对象
  CwxBinLogMgr*                      m_binlogMgr; ///<binlogMgr对象
};

///异步binlog分发的消息处理handler
class CwxMqOuterDispHandler : public CwxAppHandler4Channel {
public:
  ///构造函数
  CwxMqOuterDispHandler(CwxMqApp* pApp, CwxAppChannel* channel,
    CWX_UINT32 uiConnId);
  ///析构函数
  virtual ~CwxMqOuterDispHandler();
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
  /**
  @brief Handler的redo事件，在每次dispatch时执行。
  @return -1：处理失败，会调用close()； 0：处理成功
  */
  virtual int onRedo();

public:
  ///发送binlog。返回值：0：未发送一条binlog；1：发送了一条binlog；-1：失败；
  int syncSendBinLog(CwxMqTss* pTss);

  ///pack一条binlog。返回值：-1：失败，1：成功
  int syncPackOneBinLog(CwxPackageWriter* writer, ///<writer对象
    CwxMsgBlock*& block, ///<pack后形成的数据包
    CWX_UINT64 ullSeq, ///<消息序列号
    CwxKeyValueItem const* pData, ///<变更的数据
    char* szErr2K ///<若失败返回错误消息
    );

  ///pack多条binlog。返回值：-1：失败，1：成功
  int syncPackMultiBinLog(CwxPackageWriter* writer, ///<writer对象
    CwxPackageWriter* writer_item, ///<writer对象
    CwxKeyValueItem const* pData, ///<变更的数据
    CWX_UINT32& uiLen, ///<返回pack完当前binlog后，整个数据包的大小
    char* szErr2K ///<若失败返回错误消息
    );

  ///定位到需要的binlog处。返回值：1：发现记录；0：没有发现；-1：错误
  int syncSeekToBinlog(CwxMqTss* tss);

  ///将binlog定位到report的sid。返回值：1：成功；0：太大；-1：错误
  int syncSeekToReportSid(CwxMqTss* tss);

  ///发送export的数据。返回值：0：未发送一条数据；1：发送了一条数据；-1：失败；
  int exportSendData(CwxMqTss* pTss);

  ///获取连接id
  inline CWX_UINT32 getConnId() const {
    return m_uiConnId;
  }

public:
  ///分发线程的事件调度处理函数
  static void doEvent(CwxMqApp* app, ///<app对象
    CwxMqTss* tss, ///<线程tss
    CwxMsgBlock*& msg ///<事件消息
    );

  ///处理关闭的session
  static void dealClosedSession(CwxMqApp* app, ///<app对象
    CwxMqTss* tss  ///<线程tss
    );
  ///释放资源
  static void destroy(CwxMqApp* app);
private:
  ///收到一个消息并处理。返回值：0：成功；-1：失败
  int recvMessage();
  ///收到report的消息。返回值：0：成功；-1：失败
  int recvReport(CwxMqTss* pTss);
  ///收到new conn的report消息。返回值：0：成功；-1：失败
  int recvNewConnection(CwxMqTss* pTss);
  ///收到sync的reply消息。返回值：0：成功；-1：失败
  int recvReply(CwxMqTss* pTss);

private:
  // 是否已经报告
  bool              m_bReport;
  // 连接对应的session
  CwxMqOuterDispSession* m_syncSession;
  // session的id
  CWX_UINT64        m_ullSessionId;
  // 发送的序列号
  CWX_UINT64        m_ullSentSeq;
  // 发送的最后sid号
  CWX_UINT64        m_ullLastSid;
  // 连接id
  CWX_UINT32        m_uiConnId;
  CwxMqApp*         m_pApp;
  CwxMsgHead        m_header;
  char              m_szHeadBuf[CwxMsgHead::MSG_HEAD_LEN + 1];
  CWX_UINT32        m_uiRecvHeadLen;
  CWX_UINT32        m_uiRecvDataLen;
  CwxMsgBlock*      m_recvMsgData;
  string            m_strPeerHost;
  CWX_UINT16        m_unPeerPort;
  CwxMqTss*         m_tss;
  CwxMqZkSource     m_zkSource;
private:
  //当前分发的session
  static map<CWX_UINT64, CwxMqOuterDispSession*> m_sessions;
  // 需要关闭的session
  static list<CwxMqOuterDispSession*>            m_freeSession;
};

#endif 
