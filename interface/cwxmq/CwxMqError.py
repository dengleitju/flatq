#coding:utf-8
'''
@file CwxMqError.py
@brief MQ异常定义
@author 王淼源
@e-mail wmy123452005@sina.com
@version 1.0
@date 2011-06-27
@warning
@bug
'''

class CwxMqError(Exception):
    #协议错误代码定义
    SUCCESS          =0  #<成功
    NO_MSG           =1   #<没有数据
    INVALID_MSG      =2 #<接收到的数据包无效，也就是不是kv结构
    BINLOG_INVALID   =3#<接收到的binlog数据无效
    NO_KEY_DATA       =4 #<接收到的binlog，没有【data】的key
    INVALID_DATA_KV   =5 #<data的可以为key/value，但格式非法
    NO_SID            =6 #<接收到的report数据包中，没有【sid】的key
    NO_RET            =7 #<接收到的数据包中，没有【ret】
    NO_ERR            =8 #<接收到的数据包中，没有【err】
    NO_TIMESTAMP      =9 #<接收到的数据中，没有【timestamp】
    FAIL_AUTH         =10 #<鉴权失败
    INVALID_BINLOG_TYPE =11 #<binlog的type错误
    INVALID_MSG_TYPE   =12 #<接收到的消息类型无效
    INVALID_SID        =13  #<回复的sid无效
    FAIL_ADD_BINLOG    =14 #<往binglog mgr中添加binlog失败
    NO_QUEUE        =15 #<队列不存在
    INVALID_SUBSCRIBE =16 #<无效的消息订阅类型
    INNER_ERR        =17 #<其他内部错误，一般为内存
    INVALID_MD5      =18 #<MD5校验失败
    INVALID_CRC32    =19 #<CRC32校验失败
    NO_NAME          =20 #<没有name字段
    TIMEOUT          =21 #<commit队列类型的消息commit超时
    INVALID_COMMIT   =22 #<commit命令无效
    USER_TO0_LONG     =23 #<队列的用户名太长
    PASSWD_TOO_LONG   =24 #<队列的口令太长
    NAME_TOO_LONG   =25 #<队列名字太长
    SCRIBE_TOO_LONG   =26 #<队列订阅表达式太长
    NAME_EMPTY        =27 #<队列的名字为空
    QUEUE_EXIST       =28 #<队列存在

    def __init__(self, code, msg):
        self.code = code
        super(CwxMqError, self).__init__(msg)


class CheckSumError(Exception):
    pass

class BadPackageError(Exception):
    pass

