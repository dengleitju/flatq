#ifndef __CWX_BIN_LOG_MGR_H__
#define __CWX_BIN_LOG_MGR_H__

#include "CwxType.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"
#include "CwxCommon.h"
#include "CwxFile.h"
#include "CwxDate.h"
#include "CwxRwLock.h"
#include "CwxLockGuard.h"
#include "CwxLogger.h"
#include "CwxMqMacro.h"
#include "CwxMqDef.h"

/**
@class CwxBinLogHeader
@brief binlog的header对象定义
*/
class CwxBinLogHeader {
public:
  enum {
    BIN_LOG_HEADER_SIZE = 32 ///<serialize的空间字节数
  };
public:
  ///缺省构造函数
  inline CwxBinLogHeader();
  ///构造函数
  inline CwxBinLogHeader(CWX_UINT64 ullSid,
    CWX_UINT32 uiLogNo,
    CWX_UINT32 uiDatetime,
    CWX_UINT32 uiOffset,
    CWX_UINT32 uiLogLen,
    CWX_UINT32 uiPrevOffset,
    CWX_UINT32 uiGroup);
  ///拷贝构造
  inline CwxBinLogHeader(CwxBinLogHeader const& header);
  ///赋值操作
  inline CwxBinLogHeader& operator=(CwxBinLogHeader const& header);
  ///比较操作,true：小于；false：不小于
  inline bool operator<(CwxBinLogHeader const& header) const;
public:
  ///设置sid
  inline void setSid(CWX_UINT64 ullSid);
  ///获取sid
  inline CWX_UINT64 getSid() const;
  ///设置记录号
  inline void setLogNo(CWX_UINT32 uiLogNo);
  ///获取记录号
  inline CWX_UINT32 getLogNo() const;
  ///设置log的时间戳
  inline void setDatetime(CWX_UINT32 uiDatetime);
  ///获取log的时间戳
  inline CWX_UINT32 getDatetime() const;
  ///设置log的文件偏移
  inline void setOffset(CWX_UINT32 uiOffset);
  ///获取log的文件偏移
  inline CWX_UINT32 getOffset() const;
  ///设置log的长度，不包括log header的长度
  inline void setLogLen(CWX_UINT32 uiLogLen);
  ///获取log的长度，不包括log header的长度
  inline CWX_UINT32 getLogLen() const;
  ///设置前一个log的offset
  inline void setPrevOffset(CWX_UINT32 uiPrevOffset);
  ///获取前一个log的offset
  inline CWX_UINT32 getPrevOffset() const;
  ///设置binlog的分组
  inline void setGroup(CWX_UINT32 uiGroup);
  ///获取binlog的分组
  inline CWX_UINT32 getGroup() const;
public:
  ///将log header对象序列化，返回序列化占用的空间字节数
  inline CWX_UINT32 serialize(char* szBuf) const;
  ///将log header的buf反序列化，返回序列化占用的空间字节数
  inline CWX_UINT32 unserialize(char const* szBuf);
  ///清空对象
  inline void reset();
private:
  CWX_UINT64          m_ullSid; ///<同步序列号
  CWX_UINT32          m_uiLogNo; ///<记录号
  CWX_UINT32          m_uiDatetime; ///<记录的时间戳
  CWX_UINT32          m_uiOffset; ///<记录的文件偏移
  CWX_UINT32          m_uiLogLen; ///<记录的长度，不包括log header的长度
  CWX_UINT32          m_uiPrevOffset; ///<前一个记录的文件偏移
  CWX_UINT32          m_uiGroup; ///<binlog的group
};

/**
@class CwxBinLogIndex
@brief binlog的index对象定义
*/
class CwxBinLogIndex {
public:
  enum {
    BIN_LOG_INDEX_SIZE = 20 ///<serialize的空间字节数
  };
public:
  ///缺省构造
  inline CwxBinLogIndex();
  ///构造函数
  inline CwxBinLogIndex(CWX_UINT64 ullSid,
    CWX_UINT32 uiDatetime,
    CWX_UINT32 uiOffset,
    CWX_UINT32 uiLogLen);
  ///拷贝构造
  inline CwxBinLogIndex(CwxBinLogIndex const& index);
  ///拷贝构造
  inline CwxBinLogIndex(CwxBinLogHeader const& header);
  ///赋值操作
  inline CwxBinLogIndex& operator=(CwxBinLogIndex const& index);
  ///赋值操作
  inline CwxBinLogIndex& operator=(CwxBinLogHeader const& header);
  ///比较操作。true：小于；false：不小于
  inline bool operator<(CwxBinLogIndex const& index) const;
public:
  ///设置sid
  inline void setSid(CWX_UINT64 ullSid);
  ///获取sid
  inline CWX_UINT64 getSid() const;
  ///设置log的时间戳
  inline void setDatetime(CWX_UINT32 uiDatetime);
  ///获取log的时间戳
  inline CWX_UINT32 getDatetime() const;
  ///设置log的文件偏移
  inline void setOffset(CWX_UINT32 uiOffset);
  ///获取log的文件偏移
  inline CWX_UINT32 getOffset() const;
  ///设置log的长度，不包括log header的长度
  inline void setLogLen(CWX_UINT32 uiLogLen);
  ///获取log的长度，不包括log header的长度
  inline CWX_UINT32 getLogLen() const;
public:
  ///将log header对象序列化，返回序列化占用的空间字节数
  inline CWX_UINT32 serialize(char* szBuf) const;
  ///将log header的buf反序列化，返回序列化占用的空间字节数
  inline CWX_UINT32 unserialize(char const* szBuf);
  ///清空对象
  inline void reset();
private:
  CWX_UINT64            m_ullSid; ///<同步序列号
  CWX_UINT32            m_uiDatetime; ///<binlog的timestamp
  CWX_UINT32            m_uiOffset; ///<记录的文件偏移
  CWX_UINT32            m_uiLogLen; ///<记录的长度
};

class CwxBinLogFile;
class CwxBinLogMgr;

/**
@class CwxBinLogCursor
@brief BinLog 文件的读取游标
*/
class CwxBinLogCursor {
public:
  enum {
    BINLOG_READ_BLOCK_BIT = 16, ///<64K的读取buf。
    BINLOG_READ_BLOCK_SIZE = 1 << BINLOG_READ_BLOCK_BIT ///<64K的读取buf。
  };
  enum {
    CURSOR_STATE_UNSEEK = 0, ///<cursor处于未定位的状态
    CURSOR_STATE_READY = 1, ///<cursor处于定位的状态
    CURSOR_STATE_ERROR = 2 ///<cursor处于出错的状态
  };
public:
  ///构造函数
  CwxBinLogCursor();
  ///析构函数
  ~CwxBinLogCursor();
public:
  /**
  @brief 打开bin-log文件，此时cursor没有定位到任何记录，此时若调用next或prev，则会定位到第一个记录上。
  调用seek会定位到指定的offset上。
  @param [in] szFileName bin-log的文件名。
  @return -1：失败；0：成功
  */
  int open(char const* szFileName, CWX_UINT32 uiFileNo, CWX_UINT32 uiFileDay);
  /**
  @brief 移到下一个log
  @return -2：log的header不完整；-1：读取失败；0：当前log为最后一个log；1：移到下一个log
  */
  inline int next();
  /**
  @brief 移到前一个log
  @return -2：log的header不完整；-1：读取失败；0：当前log为第一个log；1：移到前一个log
  */
  inline int prev();
  /**
  @brief 文件偏移到指定的offset，offset的位置必须为一个log的开始位置
  @param [in] uiOffset binlog在文件中的offset。
  @return -2：log的header不完整；-1：读取失败；0：超出范围；1：移到指定的offset
  */
  inline int seek(CWX_UINT32 uiOffset);
  /**
  @brief 获取当前log的data
  @param [in] szBuf binlog的buf。
  @param [in,out] uiBufLen 传入szBuf的大小，返回读取数据的长度。
  @return  -2：数据不完成；-1：失败；>=0：获取数据的长度
  */
  int data(char * szBuf, CWX_UINT32& uiBufLen);
  ///关闭cursor
  void close();
  ///获取当期log的记录头
  inline CwxBinLogHeader const& getHeader() const;
  ///获取文件的名字
  inline string const& getFileName() const;
  ///获取文件的日期
  inline CWX_UINT32 getFileDay() const;
  ///获取文件的序号
  inline CWX_UINT32 getFileNo() const;
  ///获取当前的错误信息
  inline char* getErrMsg();
  ///获取cursor的SEEK STATE
  inline CWX_UINT8 getSeekState() const;
  ///设置cursor的SEEK STATE
  inline void setSeekState(CWX_UINT8 ucSeekState);
  ///获取cursor的SEEK SID
  inline CWX_UINT64 getSeekSid() const;
  ///设置cursor的SEEK SID
  inline void setSeekSid(CWX_UINT64 ullSid);
  ///是否ready
  inline bool isReady() const;
  ///是否unseek
  inline bool isUnseek() const;
  ///是否错误
  inline bool isError() const;
private:
  /**
  @brief 从指定位置，读取log的header。
  @param [in] uiOffset binlog在文件中的offset。
  @return -2：不存在完成的记录头；-1：失败；0：结束；1：读取一个
  */
  int header(CWX_UINT32 uiOffset);
  //获取cursor的文件 io handle
  inline int getHandle() const;
  //读取数据
  ssize_t pread(int fildes, void *buf, size_t nbyte, CWX_UINT32 offset);
  //读取一页
  bool preadPage(int fildes, CWX_UINT32 uiBlockNo, CWX_UINT32 uiOffset);
private:
  friend class CwxBinLogMgr;
  string            m_strFileName; ///<文件的名字
  int               m_fd; ///<文件的handle
  CwxBinLogHeader    m_curLogHeader; ///<当前log的header
  char              m_szHeadBuf[CwxBinLogHeader::BIN_LOG_HEADER_SIZE]; ///<log header的buf空间
  char              m_szErr2K[2048]; ///<错误信息buf
  char              m_szReadBlock[BINLOG_READ_BLOCK_SIZE];  ///<文件读取的buf
  CWX_UINT32         m_uiBlockNo;   ///<当前cache的block no
  CWX_UINT32         m_uiBlockDataOffset; ///<块中数据结束偏移
  CWX_UINT32         m_uiFileDay; ///<文件的日期
  CWX_UINT32         m_uiFileNo; ///<文件号
  //由CwxBinLogMgr使用的状态值
  CWX_UINT64         m_ullSeekSid; ///<seek的sid
  CWX_UINT8          m_ucSeekState; ///<seek的状态
};

/**
@class CwxBinLogIndexWriteCache
@brief BinLog文件的写cache对象，实现对binlog索引的写cache。
*/
class CwxBinLogIndexWriteCache {
public:
  enum {
    BINLOG_WRITE_INDEX_CACHE_RECORD_NUM = 16384 ///<写索引的320K， 20 * 16K=320K
  };
public:
  /**
  @brief 构造函数
  @param [in] indexFd 索引文件的fd handle。
  @param [in] ullIndexOffset 索引文件当前的大小。
  @param [in] ullSid  数据文件当前的最大sid。
  */
  CwxBinLogIndexWriteCache(int indexFd,
    CWX_UINT32 uiIndexOffset,
    CWX_UINT64 ullSid);
  ///析构函数
  ~CwxBinLogIndexWriteCache();

public:
  /**
  @brief 将cache的索引刷新到索引文件。
  @param [out] szErr2K 出错时的错误信息。
  @return 0:成功；-1：失败。此时索引文件写错误。
  */
  int flushIndex(char* szErr2K = NULL);
  /**
  @brief 将数据文件写到cache中。
  @param [int] header 消息的header。
  @param [out] szErr2K 出错时的错误信息。
  @return 0:成功；-1：写索引失败。
  */
  int append(CwxBinLogHeader const& header, char* szErr2K = NULL);

  friend class CwxBinLogFile;
  friend class CwxBinLogMgr;

private:
  int                 m_indexFd; ///<cache对应于索引文件的fd
  //binlog 写cache的信息
  CWX_UINT64         m_ullPrevIndexSid; ///<前一个sid，此是对应的索引文件最大的sid，若为0，表示全部在内存。
  CWX_UINT64         m_ullMinIndexSid; ///<索引cache的最小sid，若为0表示没有cache
  CWX_UINT32         m_uiIndexFileOffset; ///<索引的文件写入偏移
  unsigned char*     m_indexBuf;  ///<索引cache的buf。
  CWX_UINT32         m_uiIndexLen;  ///<索引buf的长度
  map<CWX_UINT64/*sid*/, unsigned char*> m_indexSidMap; ///<index数据的sid索引
  CWX_UINT64         m_ullMaxSid;   ///<当前最大的sid
};

/**
@class CwxBinLogFile
@brief BinLog文件关系对象，此对象负责一个binlog的数据、索引文件的管理，负责binlog文件的读操作。
*/
class CwxBinLogFile {
public:
  enum {
    SEEK_START = 0, ///<将光标移到文件的开头
    SEEK_TAIL = 1, ///<将光标移到文件的最后位置
    SEEK_SID = 2 ///<将光标移到文件的指定的SID
  };
  enum {
    MIN_BINLOG_FILE_SIZE = 32 * 1024 * 1024, ///<满的binlog文件最小为256M
    FREE_BINLOG_FILE_SIZE = 4 * 1024 * 1024 ///<binlog预留的空间为4M
  };

public:
  /**
  @brief 构造函数
  @param [in] ttDay binlog文件的日期。
  @param [in] uiFileNo binlog文件的序号。
  @param [in] uiMaxFileSize binlog文件的最大大小。
  @return 0：成功；-1：失败。
  */
  CwxBinLogFile(CWX_UINT32 ttDay,
    CWX_UINT32 uiFileNo = 0,
    CWX_UINT32 uiMaxFileSize = 512 * 1024 * 1024);
  ///析构函数
  ~CwxBinLogFile();
  ///小于比较
  inline bool operator <(CwxBinLogFile const& obj) const;

public:
  /**
  @brief 打开或创建新日志文件
  @param [in] szPathFile 日志文件名。
  @param [in] bReadOnly 是否以只读方式打开，此时，文件必须存在。
  @param [in] bCreate 是否创建新日志文件，此时，日志文件必须不存在。
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return 0：成功；-1：失败。
  */
  int open(char const* szPathFile,
    bool bReadOnly = true,
    bool bCreate = false,
    char* szErr2K = NULL);
  /**
  @brief 往日志文件添加一个新日志
  @param [in] ullSid 日志的sid号。
  @param [in] ttTimestamp 日志的日期
  @param [in] uiGroup binlog的分组
  @param [in] uiAttr 日志的属性
  @param [in] szData 日志内容
  @param [in] uiDataLen 日志长度。
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：日志文件满了；1：成功。
  */
  int append(CWX_UINT64 ullSid,
    CWX_UINT32 ttTimestamp,
    CWX_UINT32 uiGroup,
    char const* szData,
    CWX_UINT32 uiDataLen,
    char* szErr2K = NULL);
  /**
  @brief 确保将cache的数据写入到硬盘
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：成功。
  */
  int flush_cache(char* szErr2K = NULL);
  /**
  @brief 确保写入的日志保存到硬盘
  @param [in] bFlushAll true:索引数据也需要fsync到硬盘；false：只数据fsync到硬盘。
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：成功。
  */
  int fsync(bool bFlushAll = false, char* szErr2K = NULL);

  /**
  @brief 获取大于ullSid的最小binlog header
  @param [in] ullSid 要查找的sid。
  @param [out] item 满足条件的binlog index。
  @param [out] szErr2K 出错时的错误消息。
  @return -1：失败；0：不存在；1：发现
  */
  int upper(CWX_UINT64 ullSid, CwxBinLogIndex& item, char* szErr2K = NULL);

  /**
  @brief 获取不大于ullSid的最大binlog header
  @param [in] ullSid 要查找的sid。
  @param [out] item 满足条件的binlog index。
  @param [out] szErr2K 出错时的错误消息。
  @return -1：失败；0：不存在；1：发现
  */
  int lower(CWX_UINT64 ullSid, CwxBinLogIndex&item, char* szErr2K = NULL);

  /**
  @brief 定位Cursor的位置
  @param [in] cursor 日志读handle。
  @param [in] ucMode 定位的模式，SEEK_START：定位到文件的开头；SEEK_TAIL：定位到文件的最后；SEEK_SID：定位到第一个大于cursor.getSid()的日志处。
  @return -2：不存在完成的记录头；-1：失败；0：不存在；1：定位到指定的位置
  */
  int seek(CwxBinLogCursor& cursor, CWX_UINT8 ucMode = SEEK_SID);
  ///将数据trim到指定的sid，0：成功；-1：失败
  int trim(CWX_UINT64 ullSid, char* szErr2K = NULL);

  /**
  @brief 删除指定的binlog文件及其索引文件
  @param [in] szPathFileName binlog文件名。
  @return void。
  */
  static void remove(char const* szPathFileName);
  //关闭
  void close();
  /**
  @brief 获取最新的第N个记录的sid值
  @param [in] uiNo 记录号
  @param [out] ullSid sid的值
  @return true：成功；false：失败。
  */
  bool getLastSidByNo(CWX_UINT32 uiNo, CWX_UINT64& ullSid, char* szErr2K);
public:
  ///获取最小的sid
  inline CWX_UINT64 getMinSid() const;
  ///获取最大的sid
  inline CWX_UINT64 getMaxSid() const;
  ///获取binlog的最小时间戳
  inline CWX_UINT32 getMinTimestamp() const;
  ///获取binlog的最大时间戳
  inline CWX_UINT32 getMaxTimestamp() const;
  ///获取binlog文件的日期
  inline CWX_UINT32 getFileDay() const;
  ///获取binlog的文件序号
  inline CWX_UINT32 getFileNo() const;
  ///获取binlog文件的log记录数
  inline CWX_UINT32 getLogNum() const;
  ///获取binlog文件的大小
  inline CWX_UINT32 getFileSize() const;
  ///判断是否只读
  inline bool readOnly() const;
  ///设置只读
  inline void setReadOnly();
  ///判断日志文件是否为空
  inline bool empty() const;
  ///获取数据文件的名字
  inline string const& getDataFileName() const;
  ///获取索引文件的名字
  inline string const& getIndexFileName() const;
private:
  ///清空对象
  void reset();
  /**
  @brief 读取指定位置的索引记录
  @param [in] fd 索引文件的fd。
  @param [out] index 返回的索引。
  @param [in] uiOffset 索引的位置。
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：成功。
  */
  inline int readIndex(int fd,
    CwxBinLogIndex& index,
    CWX_UINT32 uiOffset,
    char* szErr2K = NULL) const;
  /**
  @brief 往指定的位置写入索引
  @param [in] fd 索引文件的fd。
  @param [in] index 写入的索引。
  @param [in] uiOffset 索引的位置。
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：成功。
  */
  inline int writeIndex(int fd, CwxBinLogIndex const& index,
    CWX_UINT32 uiOffset, char* szErr2K = NULL) const;
  /**
  @brief 创建指定的binlog文件
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：成功。
  */
  int mkBinlog(char* szErr2K = NULL);
  /**
  @brief 检查binlog文件及其索引文件是否一致，若不一致则进行处理
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：成功。
  */
  int prepareFile(char* szErr2K = NULL);
  /**
  @brief 检查是否需要创建索引
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：不需要；1：需要。
  */
  int isRebuildIndex(char* szErr2K = NULL);
  /**
  @brief 创建索引
  @param [in] szErr2K 错误信息buf，若为NULL则不返回错误消息。
  @return -1：失败；0：成功。
  */
  int createIndex(char* szErr2K = NULL);
  friend class CwxBinLogMgr;
private:
  bool                      m_bValid;       ///<是否有效
  string                    m_strPathFileName; ///<binlog文件的名字
  string                    m_strIndexFileName; ///<index文件的名字
  CWX_UINT32                m_uiMaxFileSize; ///<新建立的binlog文件的最大大小。
  CWX_UINT64                m_ullMinSid; ///<binlog文件的最小sid
  volatile CWX_UINT64       m_ullMaxSid; ///<binlog文件的最大sid
  CWX_UINT32                m_ttMinTimestamp; ///<binlog文件的log开始时间
  volatile CWX_UINT32       m_ttMaxTimestamp; ///<binlog文件的log结束时间
  volatile CWX_UINT32       m_uiLogNum; ///<binlog文件的log数量
  bool                      m_bReadOnly; ///<是否为只读
  int                       m_fd; ///<log文件的io handle
  int                       m_indexFd; ///<索引文件的io handle
  volatile CWX_UINT32       m_uiFileSize; ///<binlog数据文件大小，-1表示不存在
  volatile CWX_UINT32       m_uiIndexFileSize; ///<索引文件的大小，-1表示不存在
  volatile CWX_UINT32       m_uiPrevLogOffset; ///<前一个binlog的偏移
  CWX_UINT32                m_ttDay; ///日志文件的日期
  CWX_UINT32                m_uiFileNo; ///<日志编号。
  CwxBinLogIndexWriteCache* m_writeCache; ///<write 模式下的写cache。
};


/**
@class CwxBinLogMgr
@brief BinLog的管理对象，管理binlog的读、写。binlog的命名规则为prex_xxxxxxxxxx.log。<br>
其对应的索引文件的名字为prex_xxxxxxxxxx.log.idx。
*/
class CwxBinLogMgr {
private:
  class CwxBinLogFileItem {
  public:
    CwxBinLogFileItem(CWX_UINT32 day, CWX_UINT32 no) :
        m_uiDay(day), m_uiNo(no) {
        }
        bool operator ==(CwxBinLogFileItem const& item) const {
          return m_uiNo == item.m_uiNo;
        }
        bool operator <(CwxBinLogFileItem const& item) const {
          return m_uiNo < item.m_uiNo;
        }
  public:
    CWX_UINT32 getDay() const {
      return m_uiDay;
    }
    CWX_UINT32 getFileNo() const {
      return m_uiNo;
    }

  private:
    CWX_UINT32 m_uiDay;
    CWX_UINT32 m_uiNo;
  };
public:
  enum {
    DEF_MANAGE_FILE_NUM = 10, ///<缺省管理binlog的数量
    MIN_MANAGE_FILE_NUM = 1, ///<管理binlog的最小数量
    MAX_MANAGE_FILE_NUM = 2048, ///<管理binlog的最大小时数
    START_FILE_NUM = 1, ///<开始的文件序号
    MIN_SID_NO = 1, ///<最小的sid序号
    MAX_BINLOG_FILE_SIZE = 0X7FFFFFFF, ///<2G
    SKIP_SID_NUM = 10000 ///<当next sid小于最大值时，跳跃的数量
  };
public:
  /**
  @brief 构造函数。
  @param [in] szLogPath binlog文件所在的目录。
  @param [in] szFilePrex binlog文件的前缀，形成的文件名为szFilePrex_xxxxxxxxxx，xxxxxxxxxx为文件序号。
  @param [in] uiMaxFileSize binlog文件的最大大小。
  @param [in] uiBinlogFlushNum binlog写多少条，自动flush
  @param [in] uiBinlogFlushSecond binlog多少时间，自动flush
  @param [in] uiSaveBinlogDay binlog保存的天数
  @param [in] bDelOutManageLogFile 是否删除不再管理范围内的文件。
  @return 无。
  */
  CwxBinLogMgr(char const* szLogPath,
    char const* szFilePrex,
    CWX_UINT32 uiMaxFileSize,
    CWX_UINT32 uiBinlogFlushNum,
    CWX_UINT32 uiBinlogFlushSecond,
    CWX_UINT32 uiSaveBinlogDay);
  ///析构函数
  ~CwxBinLogMgr();
public:
  /**
  @brief 初始化binlog管理器对象。
  @param [in] uiMaxFileNum 管理的binlog的最多数量。
  @param [in] bCache 是否对写入的数据进行cache。
  @param [out] szErr2K 若初始化失败，返回失败的错误信息；若为NULL，即便失败也不返回错误的原因。
  @return -1：失败；0：成功。
  */
  int init(CWX_UINT32 uiMaxFileNum, bool bCache, char* szErr2K = NULL);
  /**
  @brief 添加一条binlog。
  @param [in] ullSid binlog的sid，其值必须大于当前已有的最大值。
  @param [in] ttTimestamp binlog的时间戳，通过此时间戳，控制binlog同步的天数。
  @param [in] uiType 日志的分组
  @param [in] uiType 日志的类型
  @param [in] szData binlog的数据。
  @param [in] uiDataLen binlog的数据的长度。
  @param [in] szErr2K 若添加失败，则为失败的原因信息。
  @return -1：失败；0：成功。
  */
  int append(CWX_UINT64& ullSid,
    CWX_UINT32 ttTimestamp,
    CWX_UINT32 uiGroup,
    char const* szData,
    CWX_UINT32 uiDataLen,
    char* szErr2K = NULL);
  /**
  @brief 刷新已经写入的binlog，确保保存到硬盘。
  @param [in] bAll 是否索引也commit。
  @param [in] szErr2K 若刷新失败，则返回失败的原因。
  @return -1：失败；0：成功。
  */
  int commit(bool bAll = false, char* szErr2K = NULL);
  // 时间commit检查；
  void timeout(CWX_UINT32 uiNow);
  ///清空binlog管理器
  void clear();
  ///清空数据
  void removeAllBinlog();
  //获取当前的应用计数
  int getRefCount();
  //新增引用计数
  int incRefCount();
  //取消一个引用
  int decRefCount();
  //获取BinlogMgr是否可删除
  bool isCanDelete(CWX_UINT32 uiNow);
  ///设置Binlog的状态
  void setZkTopicState(CWX_UINT8 state);
  ///获取Binlog的状态
  CWX_UINT8 getZkTopicState();
  ///将数据trim到指定的sid，0：成功；-1：失败
  //	int trim(CWX_UINT64 ullSid, char* szErr2K=NULL);
public:
  /**
  @brief 获取大于ullSid的最小binlog header
  @param [in] ullSid 要查找的sid。
  @param [out] index 满足条件的binlog index。
  @return -1：失败；0：不存在；1：发现
  */
  int upper(CWX_UINT64 ullSid, CwxBinLogIndex& index, char* szErr2K = NULL);
  /**
  @brief 获取不大于ullSid的最大binlog header
  @param [in] ullSid 要查找的sid。
  @param [out] index 满足条件的binlog index。
  @return -1：失败；0：不存在；1：发现
  */
  int lower(CWX_UINT64 ullSid, CwxBinLogIndex& index, char* szErr2K = NULL);

  /**
  @brief 创建binlog读取的游标
  @return NULL：失败；否则返回游标对象的指针。
  */
  CwxBinLogCursor* createCurser(CWX_UINT64 ullSid = 0,
    CWX_UINT8 ucState = CwxBinLogCursor::CURSOR_STATE_UNSEEK);

  /**
  @brief 将binlog读取的游标移到>ullSid的binlog处。
  @param [in] pCursor binlog的读取游标。
  @param [in] ullSid 定位游标的sid，游标将定位到>ullSid的binlog处。
  @return -1：失败；0：无法定位到ullSid下一个binlog；1：定位到ullSid下一个的binlog上。
  */
  int seek(CwxBinLogCursor* pCursor, CWX_UINT64 ullSid);

  /**
  @brief 将游标移到下一个binlog记录处。若有错误，则通过pCursor的getErrMsg()获取。
  @param [in] pCursor 游标的对象指针。
  @return -1：失败；0：移到最后；1：成功移到下一个binlog。
  */
  int next(CwxBinLogCursor* pCursor);
  /**
  @brief 将游标移到前一个binlog记录处。若有错误，则通过pCursor的getErrMsg()获取。
  @param [in] pCursor 游标的对象指针。
  @return -1：失败；0：移到最开始；1：成功移到前一个binlog。
  */
  int prev(CwxBinLogCursor* pCursor);

  /**
  @brief 读取游标的当前binlog。若有错误，则通过pCursor的getErrMsg()获取。
  @param [in] pCursor 游标的对象指针。
  @param [out] szData binlog的data。
  @param [in,out] uiDataLen 传入szData的buf大小，传出szData的大小。
  @return -1：失败；0：成功获取下一条binlog。
  */
  int fetch(CwxBinLogCursor* pCursor, char* szData, CWX_UINT32& uiDataLen);
  /**
  @brief 获取下一条记录。若有错误，则通过pCursor的getErrMsg()获取。
  @param [in] pCursor 游标的对象指针。
  @param [out] szData binlog的data。
  @param [in,out] uiDataLen 传入szData的buf大小，传出szData的大小。
  @return -1：失败；0：移到最后；1：成功获取下一条binlog。
  */
  int next(CwxBinLogCursor* pCursor, char* szData, CWX_UINT32& uiDataLen);
  /**
  @brief 获取前一个binlog记录。若有错误，则通过pCursor的getErrMsg()获取。
  @param [in] pCursor 游标的对象指针。
  @param [out] szData binlog的data。
  @param [in,out] uiDataLen 传入szData的buf大小，传出szData的大小。
  @return -1：失败；0：移到最开始；1：成功获取前一个binlog。
  */
  int prev(CwxBinLogCursor* pCursor, char* szData, CWX_UINT32& uiDataLen);
  /**
  @brief 释放cursor。
  @param [in] pCursor 要释放的游标。
  @return -1：失败；0：成功。
  */
  int destoryCurser(CwxBinLogCursor*& pCursor);
  /**
   @brief 获取管理cursor的数目
   @param:none
   @return: 返回管理游标的数目
   */
  int getCurserNum() {
    CwxWriteLockGuard<CwxRwLock> lock(&m_rwLock);
    return m_cursorSet.size();
  }
  /**
  @brief 获取还没有读取的日志数量。
  @param [in] pCursor 游标。
  @return -1：游标无效；否则为记录的数量。
  */
  CWX_INT64 leftLogNum(CwxBinLogCursor const* pCursor);
  /**
  @brief 获取包含指定时间日志的log文件的起始sid
  @param [in] ttTimestamp  日志时间
  @return 返回sid。
  */
  CWX_UINT64 getFileStartSid(CWX_UINT32 ttTimestamp);
  /**
  @brief 获取最新的第N个记录的sid值
  @param [in] uiNo 记录号
  @param [out] ullSid sid的值
  @return true：成功；false：失败。
  */
  bool getLastSidByNo(CWX_UINT32 uiNo, CWX_UINT64& ullSid, char* szErr2K);

public:
  ///设置初始化SID
  inline void setNextSid(CWX_UINT64 ullSid) {
    if (ullSid < getMaxSid())
      ullSid = getMaxSid() + SKIP_SID_NUM;
    m_ullNextSid = ullSid;
  }
  ///获取当前的sid
  inline CWX_UINT64 getCurNextSid() const {
    return m_ullNextSid;
  }
  ///获取管理器是否有效
  inline bool isInvalid() const;
  ///cursor对应的文件，是否在管理的范围之外
  inline bool isOutRange(CwxBinLogCursor* pCursor);
  ///是否是unseek
  inline bool isUnseek(CwxBinLogCursor* pCursor);
  ///获取管理器无效的原因
  char const* getInvalidMsg() const;
  ///获取最小的sid
  inline CWX_UINT64 getMinSid();
  ///获取最大的sid
  inline CWX_UINT64 getMaxSid();
  ///获取binlog的最小时间戳
  inline CWX_UINT32 getMinTimestamp();
  ///获取binlog的最大时间戳
  inline CWX_UINT32 getMaxTimestamp();
  ///获取管理的binlog的最小文件序号
  inline string& getMinFile(string& strFile);
  ///获取管理的binlog的最大文件序号
  inline string& getMaxFile(string& strFile);
  ///检查是否为空
  inline bool empty();
  ///获取文件号对应的binlog文件名
  inline string& getFileNameByFileNo(CWX_UINT32 uiFileNo, CWX_UINT32 ttDay,
    string& strFileName);
  ///获取文件号对应的binlog文件的索引文件名
  inline string& getIndexFileNameByFileNo(CWX_UINT32 uiFileNo,
    CWX_UINT32 ttDay, string& strFileName);
  ///根据binlog文件名，获取文件号
  inline CWX_UINT32 getBinLogFileNo(string const& strFileName,
    CWX_UINT32& ttDay);
  ///判断一个文件名是否是一个binlog文件
  inline bool isBinLogFile(string const& strFileName);
  ///判断一个文件名是否是一个binlog的索引文件
  inline bool isBinLogIndexFile(string const& strFileName);
  ///获取binlog的前缀名
  inline string const& getBinlogPrexName() const;
  ///是否有效的前缀名
  static inline bool isValidPrexName(char const* szName);

private:
  ///清空binlog管理器
  void _clear();
  /**
  @brief 获取大于ullSid的最小binlog header
  @param [in] ullSid 要查找的sid。
  @param [out] index 满足条件的binlog index。
  @return -1：失败；0：不存在；1：发现
  */
  int _upper(CWX_UINT64 ullSid, CwxBinLogIndex& index, char* szErr2K = NULL);
  /**
  @brief 获取不大于ullSid的最大binlog header
  @param [in] ullSid 要查找的sid。
  @param [out] index 满足条件的binlog index。
  @return -1：失败；0：不存在；1：发现
  */
  int _lower(CWX_UINT64 ullSid, CwxBinLogIndex& index, char* szErr2K = NULL);
  /**
  @brief 将binlog读取的游标移到>ullSid的binlog处。
  @param [in] pCursor binlog的读取游标。
  @param [in] ullSid 定位游标的sid，游标将定位到>ullSid的binlog处。
  @return -1：失败；0：无法定位到ullSid下一个binlog；1：定位到ullSid下一个的binlog上。
  */
  int _seek(CwxBinLogCursor* pCursor, CWX_UINT64 ullSid);
  ///检查一个binlog文件是否应该被管理
  inline bool _isManageBinLogFile(CwxBinLogFile* pBinLogFile);
  ///cursor对应的文件，是否在管理的范围之外
  inline bool _isOutRange(CwxBinLogCursor*& pCursor);
  ///获取最小的binlog文件
  inline CwxBinLogFile* _getMinBinLogFile();
  ///获取最大的binlog文件
  inline CwxBinLogFile* _getMaxBinLogFile();
  ///输出管理的binlog文件信息
  void _outputManageBinLog();
  ///append数据
  int _append(CWX_UINT64 ullSid,
    CWX_UINT32 ttTimestamp,
    CWX_UINT32 uiGroup,
    char const* szData,
    CWX_UINT32 uiDataLen,
    char* szErr2K = NULL);

private:
  string                      m_strLogPath; ///<binlog文件的根目录
  string                      m_strPrexLogPath; ///<指定前缀的binlog文件的目录
  string                      m_strFilePrex; ///<binlog文件的前缀名
  bool                        m_bDelOutManageLogFile; ///<是否删除不在管理内的文件
  CWX_UINT32                  m_uiMaxFileSize; ///<binlog文件的最大大小
  CWX_UINT32                  m_uiMaxFileNum; ///<管理的binlog的最大数量
  CWX_UINT32                  m_uiSaveFileDay; ///<Binlog文件的保存天数
  bool                        m_bCache;  ///<是否对写入的数据进行cache
  char                        m_szErr2K[2048]; ///<binlog 管理器无效的原因
  int                         m_fdLock; ///<系统锁文件句柄
  CwxRwLock                   m_rwLock; ///<binlog的读写锁
  ///以下变量都在读写锁保护之中
  bool                        m_bValid; ///<binlog 管理器是否有效。
  map<CWX_UINT32/*file no*/, CwxBinLogFile*> m_binlogMap; ///<包含当前binlog文件的binlog文件的map
  set<CwxBinLogCursor*>       m_cursorSet; ///<建立的所有cursor的集合
  CwxBinLogFile*              m_pCurBinlog; ///<当前写的binlog文件
  CWX_UINT64                  m_ullMinSid; ///<binlog文件的最小sid
  CWX_UINT64                  m_ullMaxSid; ///<binlog文件的最大sid
  CWX_UINT32                  m_ttMinTimestamp; ///<binlog文件的log开始时间
  CWX_UINT32                  m_ttMaxTimestamp; ///<binlog文件的log结束时间
  CWX_UINT64                  m_ullNextSid; ///<一下一个sid的值
  CWX_UINT32                  m_uiFlushBinLogNum; ///<多少binlog自动flush
  CWX_UINT32                  m_uiFlushBinLogTime; ///<多少时间自动flush
  CWX_UINT32                  m_uiUnFlushBinlog; ///<未flush的binlog数量。
  CWX_UINT32                  m_ttLastFlushBinlogTime; ///<上一次flushbinlog的时间
  ///volatile CWX_UINT32                  m_ttDeletetBinlogTime; ///<删除binlog过期时间
  CWX_UINT8                   m_cRefCount; ///<该CwxBinLogMgr的引用计数
  volatile CWX_UINT8          m_zkTopicState; ///<binlogMgr的状态
};

/**
@class CwxTopicMgr
@brief: 管理各个topic的CwxBinLogMgr。recv和dispatch调用。只有recv线程创建/删除BinlogMgr.dispatch线程
  仅仅获取BinlogMgr(同时增加应用计数)。
*/
class CwxTopicMgr
{
public:
  ///构造函数
  CwxTopicMgr(char const* szBinlogPath);
  ///析构函数
  ~CwxTopicMgr();
public:
  /**
   @brief 扫描binlog根目录初始化目录下所有topic的binlogMgr.
   @param [out] ullMaxSid 返回最大的Sid
   @param [out] uiMaxTimestamp 最大sid的时间戳
   @param [out] ullMinSid 返回最小的sid
   @param [in] uiMaxFileSize 最大binlog文件大小
   @param [in] uiBinlogFlushNum binlog文件刷新的条数
   @param [in] uiBinlogFlushSecond 刷新binlog文件的时间
   @param [in] uiMaxFileNum 最大管理binlog文件数目
   @param [in] uiSaveFileDay 保存binlog文件天数
   @param [in] bCache 是否cache索引
   @param [in] szErr2K 错误信息
   */
  int init(CWX_UINT64& ullMaxSid,
      CWX_UINT32& uiMaxTimestamp,
      CWX_UINT64& ullMinSid,
      CWX_UINT32 uiMaxFileSize,
      CWX_UINT32 uiBinlogFlushNum,
      CWX_UINT32 uiBinlogFlushSecond,
      CWX_UINT32 uiMaxFileNum,
      CWX_UINT32 uiSaveFileDay,
      bool bCache,
      char* szErr2K);
  /**
   @brief 检查topic-binlogMgr，新建/删除/冻结对应topic-binlogMgr
   @param [in] topicState 目前完整的topic的状态map,根据此来增加/删除binlogMgr
   @param [out] changedTopics 发生变化的topic/state
   @param [in] uiMaxFileSize binlog文件最大大小
   @param [in] uiBinlogFlushNum binlog刷新的数目
   @param [in] uiBinlogFlushSecond binlog刷新的时间
   @param [in] uiMaxFileNum binlog文件最大大小
   @param [in] uiSaveFileDay binlog删除后保存的天数
   @param [in] bCache 是否cache index
   @param [in] szErr2K 错误信息
   @return
   */
  int updateTopicBinlogMgr(const map<string, CWX_UINT8>& topicState,
      map<string, CWX_UINT8>& changedTopics,
      CWX_UINT32 uiMaxFileSize,
      CWX_UINT32 uiBinlogFlushNum,
      CWX_UINT32 uiBinlogFlushSecond,
      CWX_UINT32 uiMaxFileNum,
      CWX_UINT32 uiSaveFileDay,
      bool bCache=true,
      char* szErr2K = NULL);
  /**
   @brief 将被删除并且无印用的binlogMgr删除
   @param [out] delTopics 被删除的topic列表
   @return 返回被删除的topic数目
   */
  int checkTimeoutBinlogMgr(list<string>& delTopics, CWX_UINT32 uiNow);
  /**
   @brief: 根据topic获取对应的binlogMgr,增加对应的binlogMgr的引用计数
   @param[in] topic:需要获取的topic
   @param[in] bNormal 是否只需要normal的topic/binlogmgr
   @param[in] bCreate 如果不存在是否创建
   @return：返回对应的binlogMgr,无则返回NULL
   */
  CwxBinLogMgr* getBinlogMgrByTopic(string topic,
      bool bNormal=false,
      bool bCreate=false,
      char* szErr2K=NULL);
  /**
   @brief: 释放自己获取到的binlogMgr,减少引用计数
   @param[in] strTopic: binlogMgr所属的topic
   @param[in] binlogMgr:需要释放的binlogMgr
   @return void
   */
  void freeBinlogMgrByTopic(string strTopic, CwxBinLogMgr* binlogMgr) {
    ///写锁保护
    CwxWriteLockGuard<CwxRwLock> lock(&m_rwLock);
    map<string, CwxBinLogMgr*>::iterator iter = m_binlogMgrMap.find(strTopic);
    assert(iter != m_binlogMgrMap.end());
    binlogMgr->decRefCount();
  }
  /**
   @brief：获取全部topic列表
   @param[out] topics:返回获取到的topic列表
   @return:获取到topic/CwxBinlogMgr数目
   */
  int getAllTopics(list<string>& topics) {
    topics.clear();
    ///写锁保护
    CwxWriteLockGuard<CwxRwLock> lock(&m_rwLock);
    map<string, CwxBinLogMgr*>::iterator iter = m_binlogMgrMap.begin();
    while (iter != m_binlogMgrMap.end()) {
      topics.push_back(iter->first);
      iter++;
    }
    return topics.size();
  }
  /**
   @brief: 获取各个topic对应最大的sid.zk线程调用用来更新zookeeper
   @param[out] topicSid:返回获取到的topic信息
   @return: void
   */
  void dumpTopicInfo(list<CwxMqZkTopicSid>& topicSid);
  /**
   @brief commit对应topic的 binlog
   @param[in] topic:对应主题,为空表示全部
   @param[in] bAll:是否全部
   @param[out] szErr2K:错误信息
   @return -1:失败 0:成功
   */
  int commit(string topic = "", bool bAll = false, char* szErr2K = NULL);
  // 时间commit检查；
  void timeout(CWX_UINT32 uiNow);
private:
  /**
   @brief 添加一个新的topic mgr
   @param [in] topic 所属主题
   @param [in] uiMaxFileSize binlog文件的最大大小
   @param [in] uiBinlogFlushNum binlog写多少条会自动flush
   @param [in] uiBinlogFlushSecond binlog多少时间，自动flush
   @param [in] bDelOutManageLogFile 是否删除不需要管理范围内的文件。
   @return 1：添加成功 0：已经存在 -1：失败
   */
  int createBinlogMgr(string topic,
      CWX_UINT32 uiMaxFileSize,
      CWX_UINT32 uiBinlogFlushNum,
      CWX_UINT32 uiBinlogFlushSecond,
      CWX_UINT32 uiMaxFileNum,
      CWX_UINT32 uiSaveFileDay,
      CWX_UINT8 uiState,
      bool bCache=true,
      char* szErr2K = NULL);
  /**
   @brief 删除目录，递归删除所有子目录及文件
   @param[in] strPath 需要删除的目录信息
   @return 0:success -1:fail
   */
  int rmAllDir(string strPath);
private:
  string                      m_strLogRootPath; ///<各个topic的binlog文件的根目录
  CWX_UINT32                  m_uiMaxFileSize;
  CWX_UINT32                  m_uiBinlogFlushNum;
  CWX_UINT32                  m_uiBinlogFlushSecond;
  CWX_UINT32                  m_uiMaxFileNum;
  CWX_UINT32                  m_uiSaveFileDay;
  CwxRwLock                   m_rwLock; ///mgr的读写锁
  ///以下变量在锁保护之中
  map<string/*topic*/, CwxBinLogMgr*>  m_binlogMgrMap; ///<各个topic对应的binlogMgr的map
};

#include "CwxBinLogMgr.inl"

#endif
