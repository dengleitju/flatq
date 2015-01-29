#ifndef __CWX_MQ_MACRO_H__
#define __CWX_MQ_MACRO_H__
/*
版权声明：
本软件遵循GNU LGPL（http://www.gnu.org/licenses/gpl.html），
联系方式：email:cwinux@gmail.com；微博:http://t.sina.com.cn/cwinux
*/
/**
@file CwxMqMacro.h
@brief MQ系列服务的宏定义文件。
@author cwinux@gmail.com
@version 1.0
@date 2014-09-17
@warning
@bug
*/
#include "CwxGlobalMacro.h"
#include "CwxType.h"
#include "CwxStl.h"
#include "CwxStlFunc.h"

CWINUX_USING_NAMESPACE

///通信的key定义
#define CWX_MQ_D    "d"  ///<data的key
#define CWX_MQ_RET  "ret"
#define CWX_MQ_SID  "sid"
#define CWX_MQ_ERR  "err"
#define CWX_MQ_T    "t" ///<时间戳
#define CWX_MQ_U    "u" ///<user的key
#define CWX_MQ_P    "p"  ///<passwd的key
#define CWX_MQ_SOURCE "source"
#define CWX_MQ_GROUP    "g" ///<mq-group
#define CWX_MQ_CHUNK "chunk"
#define CWX_MQ_M     "m"
#define CWX_MQ_SESSION "session"
#define CWX_MQ_ZIP     "zip"
#define CWX_MQ_TOPIC   "topic"
#define CWX_MQ_STATE  "state"
///错误代码定义
#define CWX_MQ_ERR_SUCCESS          0  ///<成功
#define CWX_MQ_ERR_ERROR            1  ///<错误
#define CWX_MQ_ERR_FAIL_AUTH        2 ///<鉴权失败
#define CWX_MQ_ERR_LOST_SYNC        3 ///<失去了同步状态


#define CWX_MQ_REPORT_TIMEOUT  30  ///<report的超时时间
#define CWX_MQ_CONN_TIMEOUT_SECOND    5  ///<mq的同步连接的连接超时时间
#define CWX_MQ_MIN_TIMEOUT_SECOND     1 ///<最小的超时秒数
#define CWX_MQ_MAX_TIMEOUT_SECOND     1800 ///<最大的超时秒数
#define CWX_MQ_DEF_TIMEOUT_SECOND     5  ///<缺省的超时秒数
#define CWX_MQ_MAX_MSG_SIZE           10 * 1024 * 1024 ///<最大的消息大小
#define CWX_MQ_MAX_CHUNK_KSIZE         20 * 1024 ///<最大的chunk size
#define CWX_MQ_ZIP_EXTRA_BUF           128

#define CWX_MQ_MAX_BINLOG_FLUSH_COUNT  10000 ///<最大刷新磁盘的消息数
#define CWX_MQ_MAX_TOPIC_LEN     64 ///<topic最大长度64字节

#define CWX_MQ_MASTER_SWITCH_SID_INC 100000 ///<mq发送master切换，新master跳跃的sid的值

#define  CWX_MQ_TOPIC_NORMAL 0 ///topic正常状态
#define  CWX_MQ_TOPIC_FREEZE  1 ///topic被冻结
#define  CWX_MQ_TOPIC_DELETE  2 ///topic被删除
#endif
