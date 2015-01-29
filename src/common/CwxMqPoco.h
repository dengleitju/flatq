#ifndef __CWX_MQ_POCO_H__
#define __CWX_MQ_POCO_H__

/**
@file CwxMqPoco.h
@brief MQ系列服务的接口协议定义对象。
@author cwinux@gmail.com
@version 1.0
@date 2014-09-23
@warning
@bug
*/

#include "CwxMqMacro.h"
#include "CwxMsgBlock.h"
#include "CwxPackageReader.h"
#include "CwxPackageWriter.h"
#include "CwxCrc32.h"
#include "CwxMd5.h"

//mq的协议定义对象
class CwxMqPoco {
public:
  enum ///<消息类型定义
  {
    ///RECV服务类型的消息类型定义
    MSG_TYPE_RECV_DATA = 1, ///<数据提交消息
    MSG_TYPE_RECV_DATA_REPLY = 2, ///<数据提交消息的回复
    ///分发的消息类型定义
    MSG_TYPE_SYNC_OUTER_REPORT = 3, ///<同步SID点报告消息类型
    MSG_TYPE_SYNC_REPORT_REPLY = 4, ///<失败返回,内/外分发都是这个
    MSG_TYPE_SYNC_SESSION_REPORT = 5, ///<session的报告
    MSG_TYPE_SYNC_SESSION_REPORT_REPLY = 6, ///<session报告的回复
    MSG_TYPE_SYNC_DATA = 7,  ///<发送数据
    MSG_TYPE_SYNC_DATA_REPLY = 8, ///<数据的回复
    MSG_TYPE_SYNC_DATA_CHUNK = 9,  ///<发送数据
    MSG_TYPE_SYNC_DATA_CHUNK_REPLY = 10, ///<数据的回复
    MSG_TYPE_INNER_SYNC_REPORT = 11, ///<内部同步SID报告消息类型
    ///写代理的消息类型定义
    MSG_TYPE_PROXY_RECV_DATA = 20, ///<写代理发送数据消息
    MSG_TYPE_PROXY_RECV_DATA_REPLY = 21, ///<写代理数据发送消息回复
    ///读代理的消息类型
    MSG_TYPE_PROXY_REPORT = 22, ///<读代理同步SID点报告消息类型
    MSG_TYPE_PROXY_SYNC_DATA = 24, ///<同步数据数据
    MSG_TYPE_PROXY_SYNC_DATA_REPLY = 25, ///<同步数据返回
    MSG_TYPE_PROXY_SYNC_CHUNK_DATA = 26, ///<同步chunk数据
    MSG_TYPE_PROXY_SYNC_CHUNK_DATA_REPLY = 27, ///<同步chunk数据返回
    ///内部消息类型定义
    MSG_TYPE_MASTER_TOPIC_STATE = 50, ///<通知slave所有topic的状态
    ///错误消息
    MSG_TYPE_SYNC_ERR = 105  ///<数据同步错误消息
  };
public:
  ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packRecvData(CwxPackageWriter* writer,
    CwxMsgBlock*& msg,
    CWX_UINT32 uiTaskId,
    char const* topic,
    CwxKeyValueItem const& data,
    bool zip =false,
    char* szErr2K = NULL);

  ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseRecvData(CwxPackageReader* reader,
    CwxMsgBlock const* msg,
    CwxKeyValueItem const*& data,
    char const*& topic,
    char* szErr2K = NULL);

  ///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseRecvData(CwxPackageReader* reader,
    char const* msg,
    CWX_UINT32 msg_len,
    CwxKeyValueItem const*& data,
    char const*& topic,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packRecvDataReply(CwxPackageWriter* writer,
    CwxMsgBlock*& msg,
    CWX_UINT32 uiTaskId,
    int ret,
    CWX_UINT64 ullSid,
    char const* szErrMsg,
    char* szErr2K = NULL);
  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseRecvDataReply(CwxPackageReader* reader,
    CwxMsgBlock const* msg,
    int& ret,
    CWX_UINT64& ullSid,
    char const*& szErrMsg,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packOuterReportData(CwxPackageWriter* writer,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiTaskId,
      bool bNewly,
      CWX_UINT64 ullSid,
      char const* topic,
      CWX_UINT32 uiChunkSize,
      char const* source,
      bool zip,
      char* szErr2K=NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseOuterReportData(CwxPackageReader* reader,
      CwxMsgBlock const* msg,
      CWX_UINT64& ullSid,
      char const*& szTopic,
      bool& bNewly,
      CWX_UINT32& uiChunkSize,
      char const*& source,
      bool& zip,
      char* szErr2K=NULL);

  ///返回值:CWX_MQ_ERR_SUCCESS:成功；其他都是失败
  static int packInnerReportData(CwxPackageWriter* writer,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiTaskId,
      CWX_UINT64 ullSid,
      CWX_UINT32 uiChunkSize,
      bool zip,
      char* szErr2K=NULL);

  ///返回值：CWX_MQ_SUCCESS:成功；其他都是失败
  static int parseInnerReportData(CwxPackageReader* reader,
      CwxMsgBlock const* msg,
      CWX_UINT64& ullSid,
      CWX_UINT32& uiChunkSize,
      bool& zip,
      char* szErr2K=NULL);

  ///返回值: CWX_MQ_ERR_SUCESS:成功
  static int parseTopicItem(CwxPackageReader* reader,
    char const* szData,
    CWX_UINT32 uiDataLen,
    char const*& topic,
    CWX_UINT64& ullSid,
    char* szErr2K);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packReportDataReply(CwxPackageWriter* writer,
    CwxMsgBlock*& msg,
    CWX_UINT32 uiTaskId,
    CWX_UINT64 ullSession,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseReportDataReply(CwxPackageReader* reader,
    CwxMsgBlock const* msg,
    CWX_UINT64& ullSession,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packReportNewConn(CwxPackageWriter* writer,
    CwxMsgBlock*& msg,
    CWX_UINT32 uiTaskId,
    CWX_UINT64 ullSession,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseReportNewConn(CwxPackageReader* reader,
    CwxMsgBlock const* msg,
    CWX_UINT64& ullSession,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packSyncData(CwxPackageWriter* writer,
    CwxMsgBlock*& msg,
    CWX_UINT32 uiTaskId,
    CWX_UINT64 ullSid,
    CWX_UINT32 uiTimeStamp,
    char const* szTopic,
    CwxKeyValueItem const& data,
    bool zip,
    CWX_UINT64 ullSeq,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packSyncDataItem(CwxPackageWriter* writer,
    CWX_UINT64 ullSid,
    char const* szTopic,
    CWX_UINT32 uiTimeStamp,
    CwxKeyValueItem const& data,
    char* szErr2K = NULL);

  static int packMultiSyncData(CWX_UINT32 uiTaskId,
    char const* szData,
    CWX_UINT32 uiDataLen,
    CwxMsgBlock*& msg,
    CWX_UINT64 ullSeq,
    bool zip = false,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseSyncData(CwxPackageReader* reader,
    CwxMsgBlock const* msg,
    CWX_UINT64& ullSid,
    char const*&  szTopic,
    CWX_UINT32& uiTimeStamp,
    CwxKeyValueItem const*& data,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseSyncData(CwxPackageReader* reader,
    char const* szData,
    CWX_UINT32 uiDataLen,
    CWX_UINT64& ullSid,
    char const*&  szTopic,
    CWX_UINT32& uiTimeStamp,
    CwxKeyValueItem const*& data,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packSyncDataReply(CwxPackageWriter* writer,
    CwxMsgBlock*& msg,
    CWX_UINT32 uiTaskId,
    CWX_UINT16 unMsgType,
    CWX_UINT64 ullSeq,
    char* szErr2K = NULL);

  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseSyncDataReply(CwxPackageReader* reader,
    CwxMsgBlock const* msg,
    CWX_UINT64& ullSeq,
    char* szErr2K = NULL);

  ///pack report或sync的出错消息包。返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int packSyncErr(CwxPackageWriter* writer, ///<用于pack的writer
    CwxMsgBlock*& msg, ///<返回的消息包，对象由内部分配
    CWX_UINT32 uiTaskId, ///<消息包的task id
    int ret, ///<错误代码
    char const* szErrMsg, ///<错误消息
    char* szErr2K = NULL ///<pack出错时的错误信息
    );

  ///parse report或sync的出错数据包。返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
  static int parseSyncErr(CwxPackageReader* reader, ///<reader
    CwxMsgBlock const* msg, ///<数据包
    int& ret,  ///<错误代码
    char const*& szErrMsg,  ///<错误消息
    char* szErr2K = NULL ///<解包时的错误信息
    );

  ///pack master通知slave删除topic信息。返回值:CWX_MQ_ERR_SUCCESS: 成功；其他都是错误
  static int packTopicState(CwxPackageWriter* writer, ///<用于pack的writer
      CwxPackageWriter* itemWriter,
      CwxMsgBlock*& msg, ///<返回的消息包，对象由内部分配
      CWX_UINT32 uiTaskId, ///<消息包的task id
      map<string, CWX_UINT8> topics, ///<topic状态Map
      char* szErr2K=NULL ///<pack出错时的错误信息
      );

  ///parse master通知slave删除topic信息。
  static int parseTopicState(CwxPackageReader* reader, ///reader
      CwxPackageReader* itemReader, ///item reader
      CwxMsgBlock const* msg, ///<数据包
      map<string, CWX_UINT8>& topics, ///<topic的状态map
      char* szErr2K=NULL ///<parse出错时的错误信息
      );
  ///返回值，CWX_MQ_ERR_SUCCESS；成功；其他表示错误
  static int parseProxyRecvData(CwxPackageReader* reader,
      CwxMsgBlock const* msg,
      CwxKeyValueItem const*& data,
      char const*& group,
      char const*& topic,
      char* szErr2K=NULL);
  ///返回值：CWX_MQ_ERR_SUCCESS：成功；其他表示错误
  static int parseProxyRecvData(CwxPackageReader* reader,
      char const* msg,
      CWX_UINT32 msg_len,
      CwxKeyValueItem const*& data,
      char const*& group,
      char const*& topic,
      char* szErr2K=NULL);
  ///返回值：CWX_MQ_ERR_SUCCESS:成功；其他表示错误
  static int packProxyRecvDataReply(CwxPackageWriter* writer,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiTaskId,
      int ret,
      CWX_UINT64 ullSid,
      char const* szErrMsg,
      char* szErr2K=NULL);
  ///返回值：CWX_MQ_ERR_SUCCESS:成功；其他表示错误
  static int parseProxyReportData(CwxPackageReader* reader,
      CwxMsgBlock const* msg,
      CWX_UINT64& ullSid,
      char const*& szGroup,
      char const*& szTopic,
      bool& bNewly,
      CWX_UINT32& uiChunkSize,
      char const*& source,
      bool& zip,
      char* szErr2K=NULL);
  ///设置数据同步包的seq号
  inline static void setSeq(char* szBuf,
    CWX_UINT64 ullSeq) {
      CWX_UINT32 byte4 = (CWX_UINT32) (ullSeq >> 32);
      byte4 = CWX_HTONL(byte4);
      memcpy(szBuf, &byte4, 4);
      byte4 = (CWX_UINT32) (ullSeq & 0xFFFFFFFF);
      byte4 = CWX_HTONL(byte4);
      memcpy(szBuf + 4, &byte4, 4);

  }
  ///获取数据同步包的seq号
  inline static CWX_UINT64 getSeq(char const* szBuf) {
    CWX_UINT64 ullSeq = 0;
    CWX_UINT32 byte4;
    memcpy(&byte4, szBuf, 4);
    ullSeq = CWX_NTOHL(byte4);
    memcpy(&byte4, szBuf + 4, 4);
    ullSeq <<= 32;
    ullSeq += CWX_NTOHL(byte4);
    return ullSeq;
  }
  ///获取数据同步包的sid
  inline static CWX_UINT64 getSid(char const* szBuf) {
    CWX_UINT64 ullSid = 0;
    CWX_UINT32 byte4;
    memcpy(&byte4, szBuf, 4);
    ullSid = CWX_NTOHL(byte4);
    memcpy(&byte4, szBuf + 4, 4);
    ullSid <<= 32;
    ullSid += CWX_NTOHL(byte4);
    return ullSid;
  }

private:
  ///禁止创建对象实例
  CwxMqPoco() {
  }
  ///析构函数
  ~CwxMqPoco();
};

#endif
