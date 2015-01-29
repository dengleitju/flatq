#ifndef __CWX_SID_LOG_FILE_H__
#define __CWX_SID_LOG_FILE_H__

#include "CwxMqMacro.h"
#include "CwxMutexLock.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxCommon.h"
#include "CwxFile.h"
#include "CwxDate.h"
#include "CwxMqDef.h"

class CwxSidLogFile {
public:
  enum {
    SWITCH_FILE_LOG_NUM = 100000, ///<写入多少个Log记录，需要切换日志文件
  };
public:
  CwxSidLogFile(CWX_UINT32 uiFlushRecord, CWX_UINT32 uiFlushSecond,
    string const& strFileName);
  ~CwxSidLogFile();
public:
  // 创建log文件；0：成功；-1：失败
  int create(string const& strName, ///<对象的名字
    CWX_UINT64 ullMaxSid, ///<当前最大的sid
    string const& strUserName, ///<用户的名字
    string const& strPasswd); ///<用户的口令
  // 加载log文件；1：成功；0：不存在；-1：失败
  int load();
  // 写commit记录；0：成功；-1：失败
  int log(CWX_UINT64 sid);
  // 时间commit检查；
  void timeout(CWX_UINT32 uiNow);
  ///强行fsync日志文件；
  void syncFile();
public:
  ///删除队列文件
  inline static void removeFile(string const& file) {
    string strFile = file;
    CwxFile::rmFile(strFile.c_str());
    //remove old file
    strFile = file + ".old";
    CwxFile::rmFile(strFile.c_str());
    //remove new file
    strFile = file + ".new";
    CwxFile::rmFile(strFile.c_str());
  }
  ///获取系统文件的名字
  inline string const& getFileName() const {
    return m_strFileName;
  }
  ///获取错误信息
  inline char const* getErrMsg() const {
    return m_szErr2K;
  }
  ///是否有效
  inline bool isValid() const {
    return m_fd != NULL;
  }
  ///获取log的名字
  string const& getName() const {
    return m_strName; ///<对象的名字
  }
  ///获取当前的最大sid
  CWX_UINT64 getCurMaxSid() const {
    return m_ullMaxSid; ///<当前最大的sid
  }
  ///获取log的用户名字 
  string const& getUserName() const {
    return m_strUserName; ///<用户的名字
  }
  ///设置log的用户名
  void setUserName(string const& strUser) {
    m_strUserName = strUser;
  }
  ///获取log的用户口令
  string const& getPasswd() const {
    return m_strPasswd; ///<用户的口令
  }
  ///设置用户的口令
  void setPasswd(string const& strPasswd) {
    m_strPasswd = strPasswd;
  }

private:
  ///保存队列信息；0：成功；-1：失败
  int save();
  ///0：成功；-1：失败
  int prepare();
  ///0：成功；-1：失败
  int parseLogHead(string const& line);
  ///0：成功；-1：失败
  int parseSid(string const& line, CWX_UINT64& ullSid);
  ///关闭文件
  inline void closeFile(bool bSync = true) {
    if (m_fd) {
      if (bSync) ::fflush(m_fd);
      if (m_bLock) {
        CwxFile::unlock(fileno(m_fd));
        m_bLock = false;
      }
      fclose(m_fd);
      m_fd = NULL;
    }
  }
private:
  string                m_strFileName; ///<系统文件名字
  string                m_strOldFileName; ///<旧系统文件名字
  string                m_strNewFileName; ///<新系统文件的名字
  FILE*                 m_fd; ///<文件handle
  bool                  m_bLock; ///<文件是否已经加锁
  CWX_UINT32            m_uiFlushRecord; ///<flush硬盘的记录间隔
  CWX_UINT32            m_uiFlushSecond; ///<flush的时间间隔
  CWX_UINT32            m_uiCurLogCount; ///<自上次fsync来，log记录的次数
  CWX_UINT32            m_uiLastSyncTime; ///<上一次sync log文件的时间
  CWX_UINT32            m_uiTotalLogCount; ///<当前文件log的数量
  string                m_strName; ///<对象的名字
  CWX_UINT64            m_ullMaxSid; ///<当前最大的sid
  string                m_strUserName; ///<用户的名字
  string                m_strPasswd; ///<用户的口令
  char                  m_szErr2K[2048]; ///<错误消息
};

#endif 
