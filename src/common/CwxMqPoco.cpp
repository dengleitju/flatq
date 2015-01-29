#include "CwxMqPoco.h"
#include "CwxZlib.h"

///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packRecvData(CwxPackageWriter* writer,
                            CwxMsgBlock*& msg,
                            CWX_UINT32 uiTaskId,
                            char const* topic,
                            CwxKeyValueItem const& data,
                            bool zip,
                            char* szErr2K)
{
  writer->beginPack();
  if (!writer->addKeyValue(CWX_MQ_D, data.m_szData, data.m_uiDataLen,
    data.m_bKeyValue)) {
      if (szErr2K)
        strcpy(szErr2K, writer->getErrMsg());
      return CWX_MQ_ERR_ERROR;
  }
  if (!topic || !writer->addKeyValue(CWX_MQ_TOPIC, topic, strlen(topic))) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_RECV_DATA, uiTaskId, writer->getMsgSize());
  msg = CwxMsgBlockAlloc::malloc(
    CwxMsgHead::MSG_HEAD_LEN + writer->getMsgSize() + CWX_MQ_ZIP_EXTRA_BUF);
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  unsigned long ulDestLen = writer->getMsgSize() + CWX_MQ_ZIP_EXTRA_BUF;
  if (zip) {
    if (!CwxZlib::zip((unsigned char*) msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN,
      ulDestLen,
      (unsigned char const*) writer->getMsg(),
      writer->getMsgSize()))
    {
        zip = false;
    }
  }
  if (zip) {
    head.addAttr(CwxMsgHead::ATTR_COMPRESS);
    head.setDataLen(ulDestLen);
    memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
    msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + ulDestLen);
  } else {
    memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
    memcpy(msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN, writer->getMsg(),
      writer->getMsgSize());
    msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + writer->getMsgSize());
  }
  return CWX_MQ_ERR_SUCCESS;
}

///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvData(CwxPackageReader* reader,
                             CwxMsgBlock const* msg,
                             CwxKeyValueItem const*& data,
                             char const*& topic,
                             char* szErr2K)
{
  return parseRecvData(reader, msg->rd_ptr(), msg->length(), data, topic, szErr2K);
}

///返回值，CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvData(CwxPackageReader* reader,
                             char const* msg,
                             CWX_UINT32 msg_len,
                             CwxKeyValueItem const*& data,
                             char const*& topic,
                             char* szErr2K)
{
  if (!reader->unpack(msg, msg_len, false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  //get data
  data = reader->getKey(CWX_MQ_D);
  if (!data) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_D);
    return CWX_MQ_ERR_ERROR;
  }
  if (data->m_bKeyValue) {
    if (!CwxPackage::isValidPackage(data->m_szData, data->m_uiDataLen)) {
      if (szErr2K)
        CwxCommon::snprintf(szErr2K, 2047,
        "key[%s] is key/value, but it's format is not valid..", CWX_MQ_D);
      return CWX_MQ_ERR_ERROR;
    }
  }
  CwxKeyValueItem const* pItem = NULL;
  //get topic
  if (!(pItem = reader->getKey(CWX_MQ_TOPIC))) {
    if(szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_TOPIC);
    return CWX_MQ_ERR_ERROR;
  }
  topic = pItem->m_szData;
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packRecvDataReply(CwxPackageWriter* writer,
                                 CwxMsgBlock*& msg,
                                 CWX_UINT32 uiTaskId,
                                 int ret,
                                 CWX_UINT64 ullSid,
                                 char const* szErrMsg,
                                 char* szErr2K)
{
  writer->beginPack();
  if (!writer->addKeyValue(CWX_MQ_RET, ret)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (CWX_MQ_ERR_SUCCESS != ret) {
    if (!writer->addKeyValue(CWX_MQ_ERR, szErrMsg ? szErrMsg : "",
      szErrMsg ? strlen(szErrMsg) : 0)) {
        if (szErr2K)
          strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
  }else {
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid)) {
      if (szErr2K)
        strcpy(szErr2K, writer->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_RECV_DATA_REPLY, uiTaskId,
    writer->getMsgSize());
  msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseRecvDataReply(CwxPackageReader* reader,
                                  CwxMsgBlock const* msg,
                                  int& ret,
                                  CWX_UINT64& ullSid,
                                  char const*& szErrMsg,
                                  char* szErr2K)
{
  if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  //get ret
  if (!reader->getKey(CWX_MQ_RET, ret)) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
      CWX_MQ_RET);
    return CWX_MQ_ERR_ERROR;
  }
  //get err
  if (CWX_MQ_ERR_SUCCESS != ret) {
    CwxKeyValueItem const* pItem = NULL;
    if (!(pItem = reader->getKey(CWX_MQ_ERR))) {
      if (szErr2K)
        CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
        CWX_MQ_ERR);
      return CWX_MQ_ERR_ERROR;
    }
    szErrMsg = pItem->m_szData;
  } else {
    //get sid
    if (!reader->getKey(CWX_MQ_SID, ullSid)) {
      if (szErr2K)
        CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
        CWX_MQ_SID);
      return CWX_MQ_ERR_ERROR;
    }
    szErrMsg = "";
  }
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packOuterReportData(CwxPackageWriter* writer,
                              CwxMsgBlock*& msg,
                              CWX_UINT32 uiTaskId,
                              bool bNewly,
                              CWX_UINT64 ullSid,
                              char const* topic,
                              CWX_UINT32 uiChunkSize,
                              char const* source,
                              bool zip,
                              char* szErr2K)
{
  writer->beginPack();
  if (!topic || !writer->addKeyValue(CWX_MQ_TOPIC, topic, strlen(topic))) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!bNewly) {
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid)) {
      if (szErr2K)
        strcpy(szErr2K, writer->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
  }
  if (uiChunkSize && !writer->addKeyValue(CWX_MQ_CHUNK, uiChunkSize)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (source && !writer->addKeyValue(CWX_MQ_SOURCE, source, strlen(source))) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (zip) {
    if (!writer->addKeyValue(CWX_MQ_ZIP, zip)) {
      if (szErr2K)
        strcpy(szErr2K, writer->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_SYNC_OUTER_REPORT, uiTaskId, writer->getMsgSize());
  msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseOuterReportData(CwxPackageReader* reader,
                               CwxMsgBlock const* msg,
                               CWX_UINT64& ullSid,
                               char const*& szTopic,
                               bool& bNewly,
                               CWX_UINT32& uiChunkSize,
                               char const*& source,
                               bool& zip,
                               char* szErr2K)
{
  if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  // get sid
  bNewly = false;
  if (!reader->getKey(CWX_MQ_SID, ullSid)) {
    bNewly = true;
  }
  // get chunk
  if (!reader->getKey(CWX_MQ_CHUNK, uiChunkSize)) {
    uiChunkSize = 0;
  }
  CwxKeyValueItem const* pItem = NULL;
  //get topic
  if (!(pItem = reader->getKey(CWX_MQ_TOPIC))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in package.", CWX_MQ_TOPIC);
    return CWX_MQ_ERR_ERROR;
  }
  szTopic = pItem->m_szData;
  //get source
  if (!(pItem = reader->getKey(CWX_MQ_SOURCE))) {
    source = "";
  } else {
    source = pItem->m_szData;
  }
  CWX_UINT32 uiValue = 0;
  if (!reader->getKey(CWX_MQ_ZIP, uiValue)) {
    zip = false;
  } else {
    zip = uiValue;
  }
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packInnerReportData(CwxPackageWriter* writer,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiTaskId,
      CWX_UINT64 ullSid,
      CWX_UINT32 uiChunkSize,
      bool zip,
      char* szErr2K) {
  writer->beginPack();
  if (!writer->addKeyValue(CWX_MQ_SID, ullSid)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (uiChunkSize && !writer->addKeyValue(CWX_MQ_CHUNK, uiChunkSize)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (zip) {
    if (!writer->addKeyValue(CWX_MQ_ZIP, zip)) {
      if (szErr2K)
        strcpy(szErr2K, writer->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_INNER_SYNC_REPORT, uiTaskId, writer->getMsgSize());
  msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::parseInnerReportData(CwxPackageReader* reader,
      CwxMsgBlock const* msg,
      CWX_UINT64& ullSid,
      CWX_UINT32& uiChunkSize,
      bool& zip,
      char* szErr2K) {
  if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!reader->getKey(CWX_MQ_SID, ullSid)) {
    ullSid = 0;
  }
  // get chunk
  if (!reader->getKey(CWX_MQ_CHUNK, uiChunkSize)) {
    uiChunkSize = 0;
  }
  CWX_UINT32 uiValue = 0;
  if (!reader->getKey(CWX_MQ_ZIP, uiValue)) {
    zip = false;
  } else {
    zip = uiValue;
  }
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::parseTopicItem(CwxPackageReader* reader,
    char const* szData,
    CWX_UINT32 uiDataLen,
    char const*& topic,
    CWX_UINT64& ullSid,
    char* szErr2K)
{
  if (!reader->unpack(szData, uiDataLen, false, true)){
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
      return CWX_MQ_ERR_ERROR;
  }
  //get sid
  if (!reader->getKey(CWX_MQ_SID, ullSid)){
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in m.", CWX_MQ_SID);
    return CWX_MQ_ERR_ERROR;
  }
  //get topic
  CwxKeyValueItem const* pItem = NULL;
  //get topic
  if (!(pItem = reader->getKey(CWX_MQ_TOPIC))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in m.", CWX_MQ_TOPIC);
    return CWX_MQ_ERR_ERROR;
  }
  topic = pItem->m_szData;
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packReportDataReply(CwxPackageWriter* writer,
                                   CwxMsgBlock*& msg,
                                   CWX_UINT32 uiTaskId,
                                   CWX_UINT64 ullSession,
                                   char* szErr2K)
{
  writer->beginPack();
  if (!writer->addKeyValue(CWX_MQ_SESSION, ullSession)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_SYNC_REPORT_REPLY, uiTaskId,
    writer->getMsgSize());
  msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportDataReply(CwxPackageReader* reader,
                                    CwxMsgBlock const* msg,
                                    CWX_UINT64& ullSession,
                                    char* szErr2K)
{
  if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  //get sid
  if (!reader->getKey(CWX_MQ_SESSION, ullSession)) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
      CWX_MQ_SESSION);
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packReportNewConn(CwxPackageWriter* writer,
                                 CwxMsgBlock*& msg,
                                 CWX_UINT32 uiTaskId,
                                 CWX_UINT64 ullSession,
                                 char* szErr2K)
{
  writer->beginPack();
  if (!writer->addKeyValue(CWX_MQ_SESSION, ullSession)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_SYNC_SESSION_REPORT, uiTaskId,
    writer->getMsgSize());
  msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;

}
///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseReportNewConn(CwxPackageReader* reader,
                                  CwxMsgBlock const* msg,
                                  CWX_UINT64& ullSession,
                                  char* szErr2K)
{
  if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  //get session
  if (!reader->getKey(CWX_MQ_SESSION, ullSession)) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
      CWX_MQ_SESSION);
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncData(CwxPackageWriter* writer,
                            CwxMsgBlock*& msg,
                            CWX_UINT32 uiTaskId,
                            CWX_UINT64 ullSid,
                            CWX_UINT32 uiTimeStamp,
                            char const* szTopic,
                            CwxKeyValueItem const& data,
                            bool zip,
                            CWX_UINT64 ullSeq,
                            char* szErr2K)
{
  writer->beginPack();
  int ret = packSyncDataItem(writer, ullSid, szTopic, uiTimeStamp, data, szErr2K);
  if (CWX_MQ_ERR_SUCCESS != ret)
    return ret;

  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA, uiTaskId,
    writer->getMsgSize() + sizeof(ullSeq));

  msg = CwxMsgBlockAlloc::malloc(
    CwxMsgHead::MSG_HEAD_LEN + writer->getMsgSize() + CWX_MQ_ZIP_EXTRA_BUF
    + +sizeof(ullSeq));
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  unsigned long ulDestLen = writer->getMsgSize() + CWX_MQ_ZIP_EXTRA_BUF;
  if (zip) {
    if (!CwxZlib::zip(
      (unsigned char*) msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN
      + sizeof(ullSeq), ulDestLen,
      (unsigned char const*) writer->getMsg(), writer->getMsgSize())) {
        zip = false;
    }
  }
  if (zip) {
    head.addAttr(CwxMsgHead::ATTR_COMPRESS);
    head.setDataLen(ulDestLen + sizeof(ullSeq));
    memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
    msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + ulDestLen + sizeof(ullSeq));
  } else {
    memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
    memcpy(msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN + sizeof(ullSeq),
      writer->getMsg(), writer->getMsgSize());
    msg->wr_ptr(
      CwxMsgHead::MSG_HEAD_LEN + writer->getMsgSize() + sizeof(ullSeq));
  }
  //seq seq
  setSeq(msg->rd_ptr() + CwxMsgHead::MSG_HEAD_LEN, ullSeq);
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packSyncDataItem(CwxPackageWriter* writer,
                                CWX_UINT64 ullSid,
                                char const* szTopic,
                                CWX_UINT32 uiTimeStamp,
                                CwxKeyValueItem const& data,
                                char* szErr2K)
{
  writer->beginPack();
  if (!writer->addKeyValue(CWX_MQ_SID, ullSid)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!writer->addKeyValue(CWX_MQ_TOPIC, szTopic, strlen(szTopic))){
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!writer->addKeyValue(CWX_MQ_T, uiTimeStamp)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!writer->addKeyValue(CWX_MQ_D, data.m_szData, data.m_uiDataLen,
    data.m_bKeyValue)) {
      if (szErr2K)
        strcpy(szErr2K, writer->getErrMsg());
      return CWX_MQ_ERR_ERROR;
  }
  writer->pack();
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packMultiSyncData(CWX_UINT32 uiTaskId,
                                 char const* szData,
                                 CWX_UINT32 uiDataLen,
                                 CwxMsgBlock*& msg,
                                 CWX_UINT64 ullSeq,
                                 bool zip,
                                 char* szErr2K)
{
  CwxMsgHead head(0, 0, MSG_TYPE_SYNC_DATA_CHUNK, uiTaskId,
    uiDataLen + sizeof(ullSeq));
  msg = CwxMsgBlockAlloc::malloc(
    CwxMsgHead::MSG_HEAD_LEN + uiDataLen + CWX_MQ_ZIP_EXTRA_BUF
    + sizeof(ullSeq));
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      uiDataLen);
    return CWX_MQ_ERR_ERROR;
  }
  unsigned long ulDestLen = uiDataLen + CWX_MQ_ZIP_EXTRA_BUF;
  if (zip) {
    if (!CwxZlib::zip(
      (unsigned char*) (msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN)
      + sizeof(ullSeq), ulDestLen, (unsigned char const*) szData,
      uiDataLen)) {
        zip = false;
    }
  }
  if (zip) {
    head.addAttr(CwxMsgHead::ATTR_COMPRESS);
    head.setDataLen(ulDestLen + sizeof(ullSeq));
    memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
    msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + ulDestLen + sizeof(ullSeq));
  } else {
    memcpy(msg->wr_ptr(), head.toNet(), CwxMsgHead::MSG_HEAD_LEN);
    memcpy(msg->wr_ptr() + CwxMsgHead::MSG_HEAD_LEN + sizeof(ullSeq), szData,
      uiDataLen);
    msg->wr_ptr(CwxMsgHead::MSG_HEAD_LEN + uiDataLen + sizeof(ullSeq));
  }
  //seq seq
  setSeq(msg->rd_ptr() + CwxMsgHead::MSG_HEAD_LEN, ullSeq);
  return CWX_MQ_ERR_SUCCESS;

}

int CwxMqPoco::parseSyncData(CwxPackageReader* reader,
                             CwxMsgBlock const* msg,
                             CWX_UINT64& ullSid,
                             char const*&  szTopic,
                             CWX_UINT32& uiTimeStamp,
                             CwxKeyValueItem const*& data,
                             char* szErr2K)
{
  return parseSyncData(reader, msg->rd_ptr(), msg->length(), ullSid, szTopic,
    uiTimeStamp, data, szErr2K);
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseSyncData(CwxPackageReader* reader,
                             char const* szData,
                             CWX_UINT32 uiDataLen,
                             CWX_UINT64& ullSid,
                             char const*&  szTopic,
                             CWX_UINT32& uiTimeStamp,
                             CwxKeyValueItem const*& data,
                             char* szErr2K)
{
  if (!reader->unpack(szData, uiDataLen, false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  //get SID
  if (!reader->getKey(CWX_MQ_SID, ullSid)) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
      CWX_MQ_SID);
    return CWX_MQ_ERR_ERROR;
  }
  //get topic
  CwxKeyValueItem const* pItem = NULL;
  if (!(pItem = reader->getKey(CWX_MQ_TOPIC))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
      CWX_MQ_TOPIC);
    return CWX_MQ_ERR_ERROR;
  }
  szTopic = pItem->m_szData;
  if (strlen(szTopic) < 1){
    if(szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "Topic[%s] invalid.", szTopic);
    return CWX_MQ_ERR_ERROR;
  }

  //get source
  if (!(pItem = reader->getKey(CWX_MQ_TOPIC))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
      CWX_MQ_TOPIC);
    return CWX_MQ_ERR_ERROR;
  }
  szTopic = pItem->m_szData;
  //get timestamp
  if (!reader->getKey(CWX_MQ_T, uiTimeStamp)) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_T);
    return CWX_MQ_ERR_ERROR;
  }
  //get data
  if (!(data = reader->getKey(CWX_MQ_D))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_D);
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：CWX_MQ_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncDataReply(CwxPackageWriter* writer,
                                 CwxMsgBlock*& msg,
                                 CWX_UINT32 uiTaskId,
                                 CWX_UINT16 unMsgType,
                                 CWX_UINT64 ullSeq,
                                 char* szErr2K)
{
  char szBuf[9];
  setSeq(szBuf, ullSeq);
  CwxMsgHead head(0, 0, unMsgType, uiTaskId, sizeof(ullSeq));
  msg = CwxMsgBlockAlloc::pack(head, szBuf, sizeof(ullSeq));
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::parseSyncDataReply(CwxPackageReader*,
                                  CwxMsgBlock const* msg,
                                  CWX_UINT64& ullSeq,
                                  char* szErr2K)
{
  if (msg->length() < sizeof(ullSeq)) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047,
      "Data Length[%u] is too less, no seq id", msg->length());
    return CWX_MQ_ERR_ERROR;
  }
  ullSeq = getSeq(msg->rd_ptr());
  return CWX_MQ_ERR_SUCCESS;
}

///返回值：UNISTOR_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::packSyncErr(CwxPackageWriter* writer,
                           CwxMsgBlock*& msg,
                           CWX_UINT32 uiTaskId,
                           int ret,
                           char const* szErrMsg,
                           char* szErr2K)
{
  writer->beginPack();
  if (!writer->addKeyValue(CWX_MQ_RET, strlen(CWX_MQ_RET), ret)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (!writer->addKeyValue(CWX_MQ_ERR, strlen(CWX_MQ_ERR), szErrMsg,
    strlen(szErrMsg))) {
      if (szErr2K)
        strcpy(szErr2K, writer->getErrMsg());
      return CWX_MQ_ERR_ERROR;
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_SYNC_ERR, uiTaskId, writer->getMsgSize());
  msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;

}
///返回值：UNISTOR_ERR_SUCCESS：成功；其他都是失败
int CwxMqPoco::parseSyncErr(CwxPackageReader* reader,
                            CwxMsgBlock const* msg,
                            int& ret,
                            char const*& szErrMsg,
                            char* szErr2K)
{
  if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  //get ret
  if (!reader->getKey(CWX_MQ_RET, ret)) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
      CWX_MQ_RET);
    return CWX_MQ_ERR_ERROR;
  }
  //get err
  CwxKeyValueItem const* pItem = NULL;
  if (!(pItem = reader->getKey(CWX_MQ_ERR))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.",
      CWX_MQ_ERR);
    return CWX_MQ_ERR_ERROR;
  }
  szErrMsg = pItem->m_szData;
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packTopicState(CwxPackageWriter* writer, ///<用于pack的writer
      CwxPackageWriter* itemWriter,
      CwxMsgBlock*& msg, ///<返回的消息包，对象由内部分配
      CWX_UINT32 uiTaskId, ///<消息包的task id
      map<string, CWX_UINT8> topics, ///<topic状态Map
      char* szErr2K     ///<pack出错时的错误信息
      ) {
  writer->beginPack();
  for(map<string, CWX_UINT8>::iterator iter = topics.begin(); iter != topics.end(); iter++) {
    itemWriter->beginPack();
    if (!itemWriter->addKeyValue(CWX_MQ_TOPIC, iter->first.c_str(), iter->first.length())){
      if (szErr2K)
        strcpy(szErr2K, itemWriter->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
    if (!itemWriter->addKeyValue(CWX_MQ_STATE, iter->second)){
      if (szErr2K)
        strcpy(szErr2K, itemWriter->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
    itemWriter->pack();
    if (!writer->addKeyValue(CWX_MQ_M, itemWriter->getMsg(), itemWriter->getMsgSize(), true)){
      if (szErr2K)
        strcpy(szErr2K, itemWriter->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_MASTER_TOPIC_STATE, uiTaskId, writer->getMsgSize());
  msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::parseTopicState(CwxPackageReader* reader, ///reader
      CwxPackageReader* itemReader,
      CwxMsgBlock const* msg, ///<数据包
      map<string, CWX_UINT8>& topics, ///<topic的状态map
      char* szErr2K
      ) {
  topics.clear();
  if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  char const* topic;
  CWX_UINT8 state;
  for(CWX_UINT32 i=0; i<reader->getKeyNum(); i++) {
    if (0 != strcmp(reader->getKey(i)->m_szKey, CWX_MQ_M)) {
      if(szErr2K)
        snprintf(szErr2K, 2047, "Master topic-state's key must be:%s, but:%s",
            CWX_MQ_M, reader->getKey(i)->m_szKey);
      return CWX_MQ_ERR_ERROR;
    }
    if (!itemReader->unpack(reader->getKey(i)->m_szData, reader->getKey(i)->m_uiDataLen, false, true)) {
      if(szErr2K)
        strcpy(szErr2K, itemReader->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
    CwxKeyValueItem const* pItem = NULL;
    ///get topic
    if (!(pItem = itemReader->getKey(CWX_MQ_TOPIC))) {
      if (szErr2K)
        snprintf(szErr2K, 2047, "Master topic-state's must have topic.");
      return CWX_MQ_ERR_ERROR;
    }
    topic = pItem->m_szData;
    //get state
    if (!itemReader->getKey(CWX_MQ_STATE, state)) {
      if (szErr2K)
        snprintf(szErr2K, 2047, "Master topic-state's must have state.");
      return CWX_MQ_ERR_ERROR;
    }
    topics.insert(pair<string, CWX_UINT8>(topic, state));
  }
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::parseProxyRecvData(CwxPackageReader* reader,
      CwxMsgBlock const* msg,
      CwxKeyValueItem const*& data,
      char const*& group,
      char const*& topic,
      char* szErr2K) {
  return parseProxyRecvData(reader, msg->rd_ptr(), msg->length(), data, group, topic, szErr2K);
}

int CwxMqPoco::parseProxyRecvData(CwxPackageReader* reader,
      char const* msg,
      CWX_UINT32 msg_len,
      CwxKeyValueItem const*& data,
      char const*& group,
      char const*& topic,
      char* szErr2K) {
  if (!reader->unpack(msg, msg_len, false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  ///get data
  data = reader->getKey(CWX_MQ_D);
  if (!data) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_D);
    return CWX_MQ_ERR_ERROR;
  }
  if (data->m_bKeyValue) {
    if (!CwxPackage::isValidPackage(data->m_szData, data->m_uiDataLen)) {
      if (szErr2K)
        CwxCommon::snprintf(szErr2K, 2047,
            "Key[%s] is key/value, but it's format is not valid.", CWX_MQ_D);
      return CWX_MQ_ERR_ERROR;
    }
  }
  CwxKeyValueItem const* pItem = NULL;
  ///get group
  if (!(pItem = reader->getKey(CWX_MQ_GROUP))) {
    group = NULL;
  }else {
    group = pItem->m_szData;
  }
  ///get topic
  if (!(pItem = reader->getKey(CWX_MQ_TOPIC))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in recv page.", CWX_MQ_TOPIC);
    return CWX_MQ_ERR_ERROR;
  }
  topic = pItem->m_szData;
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::packProxyRecvDataReply(CwxPackageWriter* writer,
      CwxMsgBlock*& msg,
      CWX_UINT32 uiTaskId,
      int ret,
      CWX_UINT64 ullSid,
      char const* szErrMsg,
      char* szErr2K) {
  writer->beginPack();
  if (!writer->addKeyValue(CWX_MQ_RET, ret)) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  if (CWX_MQ_ERR_SUCCESS != ret) {
    if (!writer->addKeyValue(CWX_MQ_ERR, szErrMsg ? szErrMsg : "",
      szErrMsg ? strlen(szErrMsg) : 0)) {
        if (szErr2K)
          strcpy(szErr2K, writer->getErrMsg());
        return CWX_MQ_ERR_ERROR;
    }
  }else {
    if (!writer->addKeyValue(CWX_MQ_SID, ullSid)) {
      if (szErr2K)
        strcpy(szErr2K, writer->getErrMsg());
      return CWX_MQ_ERR_ERROR;
    }
  }
  if (!writer->pack()) {
    if (szErr2K)
      strcpy(szErr2K, writer->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  CwxMsgHead head(0, 0, MSG_TYPE_PROXY_RECV_DATA_REPLY, uiTaskId,
    writer->getMsgSize());
  msg = CwxMsgBlockAlloc::pack(head, writer->getMsg(), writer->getMsgSize());
  if (!msg) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No memory to alloc msg, size:%u",
      writer->getMsgSize());
    return CWX_MQ_ERR_ERROR;
  }
  return CWX_MQ_ERR_SUCCESS;
}

int CwxMqPoco::parseProxyReportData(CwxPackageReader* reader,
    CwxMsgBlock const* msg,
    CWX_UINT64& ullSid,
    char const*& szGroup,
    char const*& szTopic,
    bool& bNewly,
    CWX_UINT32& uiChunkSize,
    char const*& source,
    bool& zip,
    char* szErr2K) {
  if (!reader->unpack(msg->rd_ptr(), msg->length(), false, true)) {
    if (szErr2K)
      strcpy(szErr2K, reader->getErrMsg());
    return CWX_MQ_ERR_ERROR;
  }
  // get sid
  bNewly = false;
  if (!reader->getKey(CWX_MQ_SID, ullSid)) {
    bNewly = true;
  }
  // get chunk
  if (!reader->getKey(CWX_MQ_CHUNK, uiChunkSize)) {
    uiChunkSize = 0;
  }
  CwxKeyValueItem const* pItem = NULL;
  //get group
  if (!(pItem = reader->getKey(CWX_MQ_GROUP))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in package.", CWX_MQ_GROUP);
    return CWX_MQ_ERR_ERROR;
  }
  szGroup = pItem->m_szData;
  //get topic
  if (!(pItem = reader->getKey(CWX_MQ_TOPIC))) {
    if (szErr2K)
      CwxCommon::snprintf(szErr2K, 2047, "No key[%s] in package.", CWX_MQ_TOPIC);
    return CWX_MQ_ERR_ERROR;
  }
  szTopic = pItem->m_szData;
  //get source
  if (!(pItem = reader->getKey(CWX_MQ_SOURCE))) {
    source = "";
  } else {
    source = pItem->m_szData;
  }
  CWX_UINT32 uiValue = 0;
  if (!reader->getKey(CWX_MQ_ZIP, uiValue)) {
    zip = false;
  } else {
    zip = uiValue;
  }
  return CWX_MQ_ERR_SUCCESS;
}


