#include "CwxSidLogFile.h"
#include "CwxDate.h"

CwxSidLogFile::CwxSidLogFile(CWX_UINT32 uiFlushRecord,
                             CWX_UINT32 uiFlushSecond,
                             string const& strFileName)
{
  m_strFileName = strFileName; ///<系统文件名字
  m_strOldFileName = strFileName + ".old"; ///<旧系统文件名字
  m_strNewFileName = strFileName + ".new"; ///<新系统文件的名字
  m_fd = NULL;
  m_bLock = false;
  m_uiFlushRecord = uiFlushRecord; ///<flush硬盘的记录间隔
  m_uiFlushSecond = uiFlushSecond; ///<flush硬盘的时间间隔
  if (!m_uiFlushRecord)
    m_uiFlushRecord = 1;
  if (!m_uiFlushSecond)
    m_uiFlushSecond = 1;
  m_uiCurLogCount = 0; ///<自上次fsync来，log记录的次数
  m_uiTotalLogCount = 0; ///<当前文件log的数量
  m_uiLastSyncTime = time(NULL);
  strcpy(m_szErr2K, "No init");
}

CwxSidLogFile::~CwxSidLogFile() {
  closeFile(true);
}

///创建log文件；0：成功；-1：失败
int CwxSidLogFile::create(string const& strName,
                          CWX_UINT64 ullMaxSid,
                          string const& strUserName,
                          string const& strPasswd)
{
  m_strName = strName;
  m_ullMaxSid = ullMaxSid;
  m_strUserName = strUserName;
  m_strPasswd = strPasswd;
  return save();
}

///加载log文件；1：成功；0：不存在；-1：失败
int CwxSidLogFile::load() {
  m_uiLastSyncTime = time(NULL);
  if (0 != prepare()) {
    closeFile(false);
    return -1;
  }
  bool bRet = true;
  string line;
  //seek到文件头部
  fseek(m_fd, 0, SEEK_SET);
  CWX_UINT64 ullSid;
  //read log info, format:name=logname|sid=12345|u=u_q1|p=p_q1
  bRet = CwxFile::readTxtLine(m_fd, line);
  if (!bRet) {
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to read sid log file[%s], errno=%d", m_strFileName.c_str(),
      errno);
    return -1;
  }
  if (line.empty()) return 0;
  if (0 != parseLogHead(line)) return 0;
  //read sid
  while ((bRet = CwxFile::readTxtLine(m_fd, line))) {
    if (line.empty())
      break;
    //format: sid=xx
    if (0 != parseSid(line, ullSid))
      continue; ///数据可能不完整
    m_ullMaxSid = ullSid;
    m_uiTotalLogCount++;
  }
  if (!bRet) {
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to read queue file[%s], errno=%d", m_strFileName.c_str(),
      errno);
    return -1;
  }
  m_uiLastSyncTime = time(NULL);
  return 1;
}

///保存队列信息；0：成功；-1：失败
int CwxSidLogFile::save() {
  //写新文件
  int fd = ::open(m_strNewFileName.c_str(), O_RDWR | O_CREAT | O_TRUNC,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (-1 == fd) {
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to open new sys file:%s, errno=%d", m_strNewFileName.c_str(),
      errno);
    closeFile(true);
    return -1;
  }
  //写入队列信息
  char line[1024];
  char szSid[64];
  ssize_t len = 0;
  //name=log name|sid=12345|u=u_q1|p=p_q1
  len = CwxCommon::snprintf(line, 1023, "name=%s|sid=%s|u=%s|p=%s\n",
    m_strName.c_str(), CwxCommon::toString(m_ullMaxSid, szSid, 10),
    m_strUserName.c_str(), m_strPasswd.c_str());
  if (len != write(fd, line, len)) {
    ::close(fd);
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to write new sys file:%s, errno=%d", m_strNewFileName.c_str(),
      errno);
    closeFile(true);
    return -1;
  }
  ::fsync(fd);
  ::close(fd);
  //确保旧文件删除
  if (CwxFile::isFile(m_strOldFileName.c_str())
    && !CwxFile::rmFile(m_strOldFileName.c_str())) {
      CwxCommon::snprintf(m_szErr2K, 2047,
        "Failure to rm old sys file:%s, errno=%d", m_strOldFileName.c_str(),
        errno);
      closeFile(true);
      return -1;
  }
  //关闭当前文件
  closeFile(true);
  //将当前文件move为old文件
  if (CwxFile::isFile(m_strFileName.c_str())) {
    if (!CwxFile::moveFile(m_strFileName.c_str(), m_strOldFileName.c_str())) {
      CwxCommon::snprintf(m_szErr2K, 2047,
        "Failure to move current sys file:%s to old sys file:%s, errno=%d",
        m_strFileName.c_str(), m_strOldFileName.c_str(), errno);
      return -1;
    }
  }
  //将新文件移为当前文件
  if (!CwxFile::moveFile(m_strNewFileName.c_str(), m_strFileName.c_str())) {
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to move new sys file:%s to current sys file:%s, errno=%d",
      m_strNewFileName.c_str(), m_strFileName.c_str(), errno);
    return -1;
  }
  //删除旧文件
  CwxFile::rmFile(m_strOldFileName.c_str());
  //打开当前文件，接受写
  //open file
  m_fd = ::fopen(m_strFileName.c_str(), "a+");
  if (!m_fd) {
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to open sys file:[%s], errno=%d", m_strFileName.c_str(),
      errno);
    return -1;
  }
  if (!CwxFile::lock(fileno(m_fd))) {
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to lock sys file:[%s], errno=%d", m_strFileName.c_str(),
      errno);
    return -1;
  }
  m_bLock = true;
  m_uiTotalLogCount = 0;
  m_uiCurLogCount = 0;
  m_uiLastSyncTime = time(NULL);
  return 0;
}

///写commit记录；-1：失败；否则返回已经写入的log数量
int CwxSidLogFile::log(CWX_UINT64 sid) {
  if (m_fd) {
    char szBuf[128];
    char szSid[64];
    size_t len = CwxCommon::snprintf(szBuf, 127, "sid=%s\n",
      CwxCommon::toString(sid, szSid, 10));
    if (len != fwrite(szBuf, 1, len, m_fd)) {
      closeFile(false);
      CwxCommon::snprintf(m_szErr2K, 2047,
        "Failure to write log to file[%s], errno=%d", m_strFileName.c_str(),
        errno);
      return -1;
    }
    m_uiCurLogCount++;
    m_uiTotalLogCount++;
    m_ullMaxSid = sid;
    if (m_uiCurLogCount >= m_uiFlushRecord) syncFile();
    if (m_uiTotalLogCount > SWITCH_FILE_LOG_NUM) return save();
    return 0;
  }
  return -1;
}

// 时间commit检查
void CwxSidLogFile::timeout(CWX_UINT32 uiNow) {
  if ((uiNow < m_uiLastSyncTime)
    || (m_uiLastSyncTime + m_uiFlushSecond < uiNow)) {
      m_uiLastSyncTime = uiNow;
      syncFile();
  }
}

///强行fsync日志文件；
void CwxSidLogFile::syncFile() {
  if (m_uiCurLogCount && m_fd) {
    ::fflush(m_fd);
    m_uiCurLogCount = 0;
    m_uiLastSyncTime = time(NULL);
  }
}

int CwxSidLogFile::parseLogHead(string const& line) {
  list < pair<string, string> > items;
  pair < string, string > item;
  CwxCommon::split(line, items, '|');
  //get name
  if (!CwxCommon::findKey(items, "name", item)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "log file head has no [name] key.");
    return -1;
  }
  m_strName = item.second;
  //get sid
  if (!CwxCommon::findKey(items, "sid", item)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "log file head has no [sid] key.");
    return -1;
  }
  m_ullMaxSid = strtoull(item.second.c_str(), NULL, 10);
  //get user
  if (!CwxCommon::findKey(items, "u", item)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "log file head has no [u] key.");
    return -1;
  }
  m_strUserName = item.second;
  //get passwd
  if (!CwxCommon::findKey(items, "p", item)) {
    CwxCommon::snprintf(m_szErr2K, 2047, "log file head has no [p] key.");
    return -1;
  }
  m_strPasswd = item.second;
  return 0;
}

int CwxSidLogFile::parseSid(string const& line, CWX_UINT64& ullSid) {
  pair < string, string > item;
  if (!CwxCommon::keyValue(line, item))
    return -1;
  if (item.first != "sid")
    return -1;
  ullSid = strtoull(item.second.c_str(), NULL, 10);
  return 0;
}

int CwxSidLogFile::prepare() {
  bool bExistOld = CwxFile::isFile(m_strOldFileName.c_str());
  bool bExistCur = CwxFile::isFile(m_strFileName.c_str());
  bool bExistNew = CwxFile::isFile(m_strNewFileName.c_str());
  if (m_fd)
    closeFile(true);
  m_fd = NULL;
  m_uiCurLogCount = 0; ///<自上次fsync来，log记录的次数
  m_uiTotalLogCount = 0; ///<当前文件log的数量
  strcpy(m_szErr2K, "No init");
  if (!bExistCur) {
    if (bExistOld) { //采用旧文件
      if (!CwxFile::moveFile(m_strOldFileName.c_str(), m_strFileName.c_str())) {
        CwxCommon::snprintf(m_szErr2K, 2047,
          "Failure to move old sys file[%s] to cur sys file:[%s], errno=%d",
          m_strOldFileName.c_str(), m_strFileName.c_str(), errno);
        return -1;
      }
    } else if (bExistNew) { //采用新文件
      if (!CwxFile::moveFile(m_strNewFileName.c_str(), m_strFileName.c_str())) {
        CwxCommon::snprintf(m_szErr2K, 2047,
          "Failure to move new sys file[%s] to cur sys file:[%s], errno=%d",
          2047, m_strNewFileName.c_str(), m_strFileName.c_str(), errno);
        return -1;
      }
    } else { //创建空的当前文件
      int fd = ::open(m_strFileName.c_str(), O_RDWR | O_CREAT | O_TRUNC,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      if (-1 == fd) {
        CwxCommon::snprintf(m_szErr2K, 2047,
          "Failure to create cur sys file:[%s], errno=%d",
          m_strFileName.c_str(), errno);
        return -1;
      }
      ::close(fd);
    }
  }
  //open file
  m_fd = ::fopen(m_strFileName.c_str(), "a+");
  if (!m_fd) {
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to open sys file:[%s], errno=%d", m_strFileName.c_str(),
      errno);
    return -1;
  }
  if (!CwxFile::lock(fileno(m_fd))) {
    CwxCommon::snprintf(m_szErr2K, 2047,
      "Failure to lock sys file:[%s], errno=%d", m_strFileName.c_str(),
      errno);
    return -1;
  }
  //删除旧文件
  CwxFile::rmFile(m_strOldFileName.c_str());
  //删除新文件
  CwxFile::rmFile(m_strNewFileName.c_str());
  m_bLock = true;
  return 0;
}
