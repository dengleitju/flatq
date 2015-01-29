
/***********************************************************************
CwxBinLogHeader  class
***********************************************************************/
inline CwxBinLogHeader::CwxBinLogHeader()
{
  memset(this, 0x00, sizeof(this));
}

inline CwxBinLogHeader::CwxBinLogHeader(CWX_UINT64 ullSid,
                                        CWX_UINT32 uiLogNo,
                                        CWX_UINT32 uiDatetime,
                                        CWX_UINT32 uiOffset,
                                        CWX_UINT32 uiLogLen,
                                        CWX_UINT32 uiPrevOffset,
                                        CWX_UINT32 uiGroup):
m_ullSid(ullSid), m_uiLogNo(uiLogNo), m_uiDatetime(uiDatetime), m_uiOffset(uiOffset),
m_uiLogLen(uiLogLen), m_uiPrevOffset(uiPrevOffset),
m_uiGroup(uiGroup)
{
}


inline CwxBinLogHeader::CwxBinLogHeader(CwxBinLogHeader const& header){
  memcpy(this, &header, sizeof(CwxBinLogHeader));
}

inline CwxBinLogHeader& CwxBinLogHeader::operator=(CwxBinLogHeader const& header){
  if (this != &header)
  {
    memcpy(this, &header, sizeof(CwxBinLogHeader));
  }
  return *this;
}

inline bool CwxBinLogHeader::operator<(CwxBinLogHeader const& header) const{
  return this->m_ullSid<header.m_ullSid;
}

inline void CwxBinLogHeader::setSid(CWX_UINT64 ullSid){
  m_ullSid = ullSid;
}

inline CWX_UINT64 CwxBinLogHeader::getSid() const{
  return m_ullSid;
}

///设置记录号
inline void CwxBinLogHeader::setLogNo(CWX_UINT32 uiLogNo){
  m_uiLogNo = uiLogNo;
}
///获取记录号
inline CWX_UINT32 CwxBinLogHeader::getLogNo() const{
  return m_uiLogNo;
}

inline void CwxBinLogHeader::setDatetime(CWX_UINT32 uiDatetime){
  m_uiDatetime = uiDatetime;
}

inline CWX_UINT32 CwxBinLogHeader::getDatetime() const{
  return m_uiDatetime;
}

inline void CwxBinLogHeader::setOffset(CWX_UINT32 uiOffset){
  m_uiOffset = uiOffset;
}

inline CWX_UINT32 CwxBinLogHeader::getOffset() const{
  return m_uiOffset;
}

inline void CwxBinLogHeader::setLogLen(CWX_UINT32 uiLogLen){
  m_uiLogLen = uiLogLen;
}

inline CWX_UINT32 CwxBinLogHeader::getLogLen() const{
  return m_uiLogLen;
}

inline void CwxBinLogHeader::setPrevOffset(CWX_UINT32 uiPrevOffset){
  m_uiPrevOffset = uiPrevOffset;
}

inline CWX_UINT32 CwxBinLogHeader::getPrevOffset() const{
  return m_uiPrevOffset;
}

///设置binlog的分组
inline void CwxBinLogHeader::setGroup(CWX_UINT32 uiGroup){
  m_uiGroup = uiGroup;
}

///获取binlog的分组
inline CWX_UINT32 CwxBinLogHeader::getGroup() const{
  return m_uiGroup;
}

inline CWX_UINT32 CwxBinLogHeader::serialize(char* szBuf) const{
  CWX_UINT32 byte4;
  CWX_UINT8  pos = 0;
  //sid
  byte4 = (CWX_UINT32)(m_ullSid>>32);
  byte4 = CWX_HTONL(byte4); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  byte4 = (CWX_UINT32)(m_ullSid&0xFFFFFFFF);
  byte4 = CWX_HTONL(byte4); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //logno
  byte4 = CWX_HTONL(m_uiLogNo); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //datetime
  byte4 = CWX_HTONL(m_uiDatetime); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //offset
  byte4 = CWX_HTONL(m_uiOffset); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //log-length
  byte4 = CWX_HTONL(m_uiLogLen); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //prev-offset
  byte4 = CWX_HTONL(m_uiPrevOffset); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //group
  byte4 = CWX_HTONL(m_uiGroup); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  return pos;
}

inline CWX_UINT32 CwxBinLogHeader::unserialize(char const* szBuf){
  CWX_UINT32 byte4;
  CWX_UINT8  pos = 0;
  //sid
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_ullSid = CWX_NTOHL(byte4);
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_ullSid <<=32;
  m_ullSid += CWX_NTOHL(byte4);
  //log no
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiLogNo = CWX_NTOHL(byte4);
  //datetime
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiDatetime = CWX_NTOHL(byte4);
  //offset
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiOffset = CWX_NTOHL(byte4);
  //log-length
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiLogLen = CWX_NTOHL(byte4);
  //prev-offset
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiPrevOffset = CWX_NTOHL(byte4);
  //group
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiGroup = CWX_NTOHL(byte4);
  return pos;
}

///清空对象
inline void CwxBinLogHeader::reset(){
  memset(this, 0x00, sizeof(CwxBinLogHeader));
}

/***********************************************************************
CwxBinLogIndex  class
***********************************************************************/
inline CwxBinLogIndex::CwxBinLogIndex()
{
  memset(this, 0x00, sizeof(CwxBinLogIndex));
}

inline CwxBinLogIndex::CwxBinLogIndex(CWX_UINT64 ullSid,
                                      CWX_UINT32 uiDatetime,
                                      CWX_UINT32 uiOffset,
                                      CWX_UINT32 uiLogLen):
m_ullSid(ullSid), m_uiDatetime(uiDatetime), m_uiOffset(uiOffset), m_uiLogLen(uiLogLen)
{
}

inline CwxBinLogIndex::CwxBinLogIndex(CwxBinLogIndex const& index)
{
  memcpy(this, &index, sizeof(CwxBinLogIndex));
}

inline CwxBinLogIndex::CwxBinLogIndex(CwxBinLogHeader const& header)
{
  m_ullSid = header.getSid();
  m_uiDatetime = header.getDatetime();
  m_uiOffset = header.getOffset();
  m_uiLogLen = header.getLogLen();
}

inline CwxBinLogIndex& CwxBinLogIndex::operator=(CwxBinLogIndex const& index)
{
  if (this != &index)
  {
    memcpy(this, &index, sizeof(CwxBinLogIndex));
  }
  return *this;
}

inline CwxBinLogIndex& CwxBinLogIndex::operator=(CwxBinLogHeader const& header)
{
  m_ullSid = header.getSid();
  m_uiDatetime = header.getDatetime();
  m_uiOffset = header.getOffset();
  m_uiLogLen = header.getLogLen();
  return *this;
}

inline bool CwxBinLogIndex::operator<(CwxBinLogIndex const& index) const
{
  return m_ullSid < index.m_ullSid;
}

inline void CwxBinLogIndex::setSid(CWX_UINT64 ullSid)
{
  m_ullSid = ullSid;
}

inline CWX_UINT64 CwxBinLogIndex::getSid() const
{
  return m_ullSid;
}

inline void CwxBinLogIndex::setDatetime(CWX_UINT32 uiDatetime)
{
  m_uiDatetime = uiDatetime;
}

inline CWX_UINT32 CwxBinLogIndex::getDatetime() const
{
  return m_uiDatetime;
}


inline void CwxBinLogIndex::setOffset(CWX_UINT32 uiOffset)
{
  m_uiOffset = uiOffset;
}

inline CWX_UINT32 CwxBinLogIndex::getOffset() const
{
  return m_uiOffset;
}

inline void CwxBinLogIndex::setLogLen(CWX_UINT32 uiLogLen)
{
  m_uiLogLen = uiLogLen;
}

inline CWX_UINT32 CwxBinLogIndex::getLogLen() const
{
  return m_uiLogLen;
}

inline CWX_UINT32 CwxBinLogIndex::serialize(char* szBuf) const
{
  CWX_UINT32 byte4;
  CWX_UINT8  pos = 0;
  //sid
  byte4 = (CWX_UINT32)(m_ullSid>>32);
  byte4 = CWX_HTONL(byte4); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  byte4 = (CWX_UINT32)(m_ullSid&0xFFFFFFFF);
  byte4 = CWX_HTONL(byte4); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //datetime
  byte4 = CWX_HTONL(m_uiDatetime); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //offset
  byte4 = CWX_HTONL(m_uiOffset); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  //log-length
  byte4 = CWX_HTONL(m_uiLogLen); memcpy(szBuf+pos, &byte4, 4); pos+=4;
  return pos;
}

inline CWX_UINT32 CwxBinLogIndex::unserialize(char const* szBuf)
{
  CWX_UINT32 byte4;
  CWX_UINT8  pos = 0;
  //sid
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_ullSid = CWX_NTOHL(byte4);
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_ullSid <<=32;
  m_ullSid += CWX_NTOHL(byte4);
  //datetime
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiDatetime = CWX_NTOHL(byte4);
  //offset
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiOffset = CWX_NTOHL(byte4);
  //log-length
  memcpy(&byte4, szBuf+pos, 4); pos += 4;
  m_uiLogLen = CWX_NTOHL(byte4);
  return pos;
}

///清空对象
inline void CwxBinLogIndex::reset()
{
  memset(this, 0x00, sizeof(CwxBinLogIndex));
}



/***********************************************************************
CwxBinLogCursor  class
***********************************************************************/
inline int CwxBinLogCursor::next()
{
  if (CURSOR_STATE_ERROR == m_ucSeekState) return -1;
  if (-1 == m_fd){
    CwxCommon::snprintf(this->m_szErr2K, 2047, "Cursor's file handle is invalid");
    return -1;
  }
  if (CURSOR_STATE_UNSEEK == m_ucSeekState) {
    return header(0);
  }
  return header(m_curLogHeader.getOffset() + CwxBinLogHeader::BIN_LOG_HEADER_SIZE + m_curLogHeader.getLogLen());
}

inline int CwxBinLogCursor::prev()
{
  if (CURSOR_STATE_ERROR == m_ucSeekState) return -1;
  if (-1 == m_fd){
    CwxCommon::snprintf(this->m_szErr2K, 2047, "Cursor's file handle is invalid");
    return -1;
  }
  if (CURSOR_STATE_UNSEEK == m_ucSeekState) {
    return header(0);
  }
  if (0 != m_curLogHeader.getOffset()){
    return header(m_curLogHeader.getPrevOffset());
  }
  return 0;
}

inline int CwxBinLogCursor::seek(CWX_UINT32 uiOffset)
{
  m_ucSeekState = CURSOR_STATE_UNSEEK;
  if (-1 == m_fd)
  {
    CwxCommon::snprintf(this->m_szErr2K, 2047, "Cursor's file handle is invalid");
    return -1;
  }
  int iRet = header(uiOffset);
  if (1 == iRet){
    m_ucSeekState = CURSOR_STATE_READY;
  }
  return iRet;
}


inline CwxBinLogHeader const& CwxBinLogCursor::getHeader() const
{
  return m_curLogHeader;
}

inline string const& CwxBinLogCursor::getFileName() const
{
  return m_strFileName;
}

///获取文件的日期
inline CWX_UINT32 CwxBinLogCursor::getFileDay() const
{
  return m_uiFileDay;
}

///获取文件的序号
inline CWX_UINT32 CwxBinLogCursor::getFileNo() const
{
  return m_uiFileNo;
}


inline char* CwxBinLogCursor::getErrMsg()
{
  return m_szErr2K;
}

inline int CwxBinLogCursor::getHandle() const
{
  return m_fd;
}

///获取cursor的SEEK STATE
inline CWX_UINT8 CwxBinLogCursor::getSeekState() const{
  return m_ucSeekState;
}
///设置cursor的SEEK STATE
inline void CwxBinLogCursor::setSeekState(CWX_UINT8 ucSeekState){
  m_ucSeekState = ucSeekState;
}
///获取cursor的SEEK SID
inline CWX_UINT64 CwxBinLogCursor::getSeekSid() const{
  return m_ullSeekSid;
}
///设置cursor的SEEK SID
inline void CwxBinLogCursor::setSeekSid(CWX_UINT64 ullSid){
  m_ullSeekSid = ullSid;
}

///是否ready
inline bool CwxBinLogCursor::isReady() const{
  return CURSOR_STATE_READY == m_ucSeekState;
}
///是否unseek
inline bool CwxBinLogCursor::isUnseek() const{
  return CURSOR_STATE_UNSEEK == m_ucSeekState;
}
///是否错误
inline bool CwxBinLogCursor::isError() const{
  return CURSOR_STATE_ERROR == m_ucSeekState;
}


/***********************************************************************
CwxBinLogFile  class
***********************************************************************/
///小于比较
inline bool CwxBinLogFile::operator < (CwxBinLogFile const& obj) const
{
  return m_ullMinSid < obj.m_ullMaxSid;
}

///获取最小的sid
inline CWX_UINT64 CwxBinLogFile::getMinSid() const
{
  return m_ullMinSid;
}

///获取最大的sid
inline CWX_UINT64 CwxBinLogFile::getMaxSid() const
{
  return m_ullMaxSid;
}

///获取binlog的最小时间戳
inline CWX_UINT32 CwxBinLogFile::getMinTimestamp() const
{
  return m_ttMinTimestamp;
}

///获取binlog的最大时间戳
inline CWX_UINT32 CwxBinLogFile::getMaxTimestamp() const
{
  return m_ttMaxTimestamp;
}

inline CWX_UINT32 CwxBinLogFile::getFileDay() const
{
  return m_ttDay;
}

///获取binlog的文件序号
inline CWX_UINT32 CwxBinLogFile::getFileNo() const
{
  return m_uiFileNo;
}

///获取binlog文件的log记录数
inline CWX_UINT32 CwxBinLogFile::getLogNum() const
{
  return m_uiLogNum;
}

///获取binlog文件的大小
inline CWX_UINT32 CwxBinLogFile::getFileSize() const
{
  return m_uiFileSize;
}

///判断是否只读
inline bool CwxBinLogFile::readOnly() const
{
  return m_bReadOnly;
}

///设置只读
inline void CwxBinLogFile::setReadOnly()
{
  if (!m_bReadOnly)
  {
    flush_cache(NULL);
    fsync(true,NULL);
    m_bReadOnly = true;
    if (-1 != m_fd) ::close(m_fd);
    m_fd = -1;
    if (-1 != m_indexFd) ::close(m_indexFd);
    m_indexFd = -1;
    if (m_writeCache) delete m_writeCache;
    m_writeCache = NULL;

  }
}

///判断日志文件是否为空
inline bool CwxBinLogFile::empty() const
{
  return !m_uiLogNum;
}

///获取数据文件的名字
inline string const& CwxBinLogFile::getDataFileName() const
{
  return m_strPathFileName;
}
///获取索引文件的名字
inline string const& CwxBinLogFile::getIndexFileName() const
{
  return m_strIndexFileName;
}


//-1：失败；0：成功。
inline int CwxBinLogFile::readIndex(int fd, CwxBinLogIndex& index, CWX_UINT32 uiOffset, char* szErr2K) const
{
  char szBuf[CwxBinLogIndex::BIN_LOG_INDEX_SIZE];
  ssize_t ret = pread(fd, &szBuf, CwxBinLogIndex::BIN_LOG_INDEX_SIZE, uiOffset);
  if (CwxBinLogIndex::BIN_LOG_INDEX_SIZE != ret)
  {
    if (szErr2K)
    {
      if (-1 == ret)
      {
        CwxCommon::snprintf(szErr2K, 2047, "Failure to read binlog index, file:%s, errno=%d", this->m_strIndexFileName.c_str(), errno);
      }
      else
      {
        CwxCommon::snprintf(szErr2K, 2047, "No complete index record, offset:%u", uiOffset);
      }
    }
    return -1;
  }
  index.unserialize(szBuf);
  return 0;
}

// -1：失败；0：成功。
inline int CwxBinLogFile::writeIndex(int fd, CwxBinLogIndex const& index, CWX_UINT32 uiOffset, char* szErr2K) const
{
  char szBuf[CwxBinLogIndex::BIN_LOG_INDEX_SIZE];
  index.serialize(szBuf);
  if (CwxBinLogIndex::BIN_LOG_INDEX_SIZE != pwrite(fd, szBuf, CwxBinLogIndex::BIN_LOG_INDEX_SIZE, uiOffset))
  {
    if (szErr2K) CwxCommon::snprintf(szErr2K, 2047, "Failure to write binlog index, file:%s, errno=%d", m_strIndexFileName.c_str(), errno);
    return -1;
  }
  return 0;
}

/***********************************************************************
CwxBinLogMgr  class
***********************************************************************/
///获取管理器是否有效
inline bool CwxBinLogMgr::isInvalid() const
{
  return !m_bValid;
}

inline bool CwxBinLogMgr::isOutRange(CwxBinLogCursor* pCursor)
{
  CwxReadLockGuard<CwxRwLock> lock(&m_rwLock);
  return _isOutRange(pCursor);
}
///是否是unseek
inline bool CwxBinLogMgr::isUnseek(CwxBinLogCursor* pCursor)
{
  CwxReadLockGuard<CwxRwLock> lock(&m_rwLock);
  return pCursor->isUnseek();
}

///cursor对应的文件，是否在管理的范围之外
inline bool CwxBinLogMgr::_isOutRange(CwxBinLogCursor*& pCursor)
{
  bool ret = pCursor->getHeader().getSid() < getMinSid();
  CWX_ASSERT(!ret);
  return ret;
}


///获取管理器无效的原因
inline char const* CwxBinLogMgr::getInvalidMsg() const
{
  return m_szErr2K;
}

///获取最小的sid
inline CWX_UINT64 CwxBinLogMgr::getMinSid()
{
  return m_ullMinSid;
}
///获取最大的sid
inline CWX_UINT64 CwxBinLogMgr::getMaxSid()
{
  return m_ullMaxSid;
}
///获取binlog的最小时间戳
inline CWX_UINT32 CwxBinLogMgr::getMinTimestamp()
{
  return m_ttMinTimestamp;
}

///获取binlog的最大时间戳
inline CWX_UINT32 CwxBinLogMgr::getMaxTimestamp()
{
  return m_ttMaxTimestamp;
}

///获取管理的binlog的最小文件序号
inline string& CwxBinLogMgr::getMinFile(string& strFile)
{
  ///读锁保护
  CwxReadLockGuard<CwxRwLock> lock(&m_rwLock);
  CwxBinLogFile* pFile = _getMinBinLogFile();
  strFile = pFile?pFile->getDataFileName():"";
  return strFile;
}
///获取管理的binlog的最大文件序号
inline string& CwxBinLogMgr::getMaxFile(string& strFile)
{
  ///读锁保护
  CwxReadLockGuard<CwxRwLock> lock(&m_rwLock);
  CwxBinLogFile* pFile = _getMaxBinLogFile();
  strFile = pFile?pFile->getDataFileName():"";
  return strFile;
}
///检查是否为空
inline bool CwxBinLogMgr::empty()
{
  return !getMaxSid();
}
///获取文件号对应的binlog文件名
inline string& CwxBinLogMgr::getFileNameByFileNo(CWX_UINT32 uiFileNo,
                                                 CWX_UINT32 ttDay,
                                                 string& strFileName)
{
  char szPathFile[512];
  string strDay;
  CwxDate::getDateY4MDHMS2(ttDay, strDay);
  snprintf(szPathFile, 511, "%s%s.%10.10d.%s.log",
    m_strPrexLogPath.c_str(),
    m_strFilePrex.c_str(),
    uiFileNo,
    strDay.substr(0,8).c_str());
  strFileName = szPathFile;
  return strFileName;
}
///获取文件号对应的binlog文件的索引文件名
inline string& CwxBinLogMgr::getIndexFileNameByFileNo(CWX_UINT32 uiFileNo,
                                                      CWX_UINT32 ttDay,
                                                      string& strFileName)
{
  getFileNameByFileNo(uiFileNo, ttDay, strFileName);
  strFileName += ".idx";
  return strFileName;
}

inline CWX_UINT32 CwxBinLogMgr::getBinLogFileNo(string const& strFileName, CWX_UINT32& ttDay)
{

  if (strFileName.length() <= m_strPrexLogPath.length() + m_strFilePrex.length() + 13) return 0;
  string strFileNum = strFileName.substr(m_strPrexLogPath.length() + m_strFilePrex.length() + 1, 10);
  string strDay = strFileName.substr(m_strPrexLogPath.length() + m_strFilePrex.length() + 1 + 10 + 1, 8);
  strDay += "000000";
  ttDay = CwxDate::getDateY4MDHMS2(strDay);
  return strtoul(strFileNum.c_str(), NULL, 10);
}

inline bool CwxBinLogMgr::isBinLogFile(string const& strFileName)
{
  string strTmp;
  CWX_UINT32  ttDay=0;
  CWX_UINT32 uiFileNo = getBinLogFileNo(strFileName, ttDay);
  return getFileNameByFileNo(uiFileNo, ttDay, strTmp) == strFileName;
}

///判断一个文件名是否是一个binlog的索引文件
inline bool CwxBinLogMgr::isBinLogIndexFile(string const& strFileName)
{
  string strTmp;
  CWX_UINT32 ttDay = 0;
  CWX_UINT32 uiFileNo = getBinLogFileNo(strFileName, ttDay);
  return getIndexFileNameByFileNo(uiFileNo, ttDay, strTmp) == strFileName;
}
///获取binlog的前缀名
inline  string const& CwxBinLogMgr::getBinlogPrexName() const
{
  return m_strFilePrex;
}
///是否有效的前缀名
inline bool CwxBinLogMgr::isValidPrexName(char const* szName)
{
  if (!szName) return false;
  if (!strlen(szName)) return false;
  size_t i=0;
  while(szName[i])
  {
    if (((szName[i] >= 'a') && (szName[i]<='z')) ||
      ((szName[i] >= 'A') && (szName[i]<='Z')) ||
      ((szName[i] >= '0') && (szName[i]<='9')) ||
      (szName[i] == '.') || (szName[i] == '_') || (szName[i] == '-'))
      continue;
    return false;
  }
  return true;
}


inline CwxBinLogFile* CwxBinLogMgr::_getMinBinLogFile() 
{
  if (m_binlogMap.begin() != m_binlogMap.end()) return m_binlogMap.begin()->second;
  return m_pCurBinlog;
}

inline CwxBinLogFile* CwxBinLogMgr::_getMaxBinLogFile()
{
  return m_pCurBinlog;
}

inline bool CwxBinLogMgr::_isManageBinLogFile(CwxBinLogFile* pBinLogFile)
{
  if (!m_pCurBinlog) return true;
  ///如果文件被cursor使用，则被管理
  if (m_binlogMap.size() <= m_uiMaxFileNum) return true;
  //检测是否有cursor在使用
  set<CwxBinLogCursor*>::iterator iter = m_cursorSet.begin();
  while(iter != m_cursorSet.end()){
    if ((*iter)->isReady()){//cursor的header一定有效
      if ((*iter)->getHeader().getSid() <= pBinLogFile->getMaxSid()){
        return true;
      }
    }else if ((*iter)->getSeekSid() <= pBinLogFile->getMaxSid()){//cursor处于悬浮状态
      return true;
    }
    iter++;
  }
  return false;

}

inline int CwxBinLogMgr::getRefCount() {
  ///写锁保护
  CwxWriteLockGuard<CwxRwLock> lock(&m_rwLock);
  return m_cRefCount;
}

inline int CwxBinLogMgr::incRefCount() {
  ///写锁保护
  CwxWriteLockGuard<CwxRwLock> lock(&m_rwLock);
  return ++m_cRefCount;
}

inline int CwxBinLogMgr::decRefCount() {
  ///写锁保护
  CwxWriteLockGuard<CwxRwLock> lock(&m_rwLock);
  return --m_cRefCount;
}


///对于已经删除状态binlogMgr,判断是否可以彻底删除
inline bool CwxBinLogMgr::isCanDelete(CWX_UINT32 uiNow) {
 if(getRefCount() != 0 || m_zkTopicState != CWX_MQ_TOPIC_DELETE) return false;
 if (m_ttMaxTimestamp + m_uiSaveFileDay * 86400 < uiNow) return true;
 return false;
}

///设置Binlog的topicConf的配置信息
inline void CwxBinLogMgr::setZkTopicState(CWX_UINT8 state) {
  ///写锁保护
  CwxWriteLockGuard<CwxRwLock> lock(&m_rwLock);
  m_zkTopicState = state;
}

///获取zk配置信息
inline CWX_UINT8 CwxBinLogMgr::getZkTopicState() {
  ///写锁保护
  CwxWriteLockGuard<CwxRwLock> lock(&m_rwLock);
  return m_zkTopicState;
}