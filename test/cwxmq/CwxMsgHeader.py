#coding:utf-8
'''
@file CwxMsgHeader.py
@brief 定义MQ消息头
@author 王淼源
@e-mail wmy123452005@sina.com
@version 1.0
@date 2011-06-27
@warning
@bug
'''

import struct
from CwxMqError import CheckSumError

class CwxMsgHeader:
    #定义通信包头的长度
    HEAD_LEN = 14

    #定义系统消息的属性
    ATTR_SYS_MSG = (1<<0)
    #定义数据压缩的属性
    ATTR_COMPRESS = (1<<1)

    #keep alive的消息类型
    TYPE_KEEPALIVE = 1
    #keep alive回复的消息类型
    TYPE_KEEPALIVE_REPLY = 2

    def __init__(self, head=None):
        '''
        @brief 构造一个消息头，解成消息头对象。
        @param [in] head 消息包头的buf，解析此buf，如果为空则构造一个空消息头
        @return 包头对象
        '''
        if not head:
            self.version = 0 #<消息版本号
            self.attr = 0     #<消息属性
            self.msg_type = 0  #<消息类型
            self.task_id = 0   #<任务号ID
            self.data_len = 0  #<发送的数据长度
        else:
            res = struct.unpack("!BBHIIH", head)
            args = res[:-1]
            check_sum = 0
            for i in args:
                check_sum += i
            (self.version, self.attr, self.msg_type, self.task_id, self.data_len) = args
            '''if check_sum == res[-1]:
                (self.version, self.attr, self.msg_type, self.task_id, self.data_len) = args
            else:
                raise CheckSumError'''

    def __str__(self):
        '''
        @brief 将消息头打包成网络字节序的数据包返回
        @return 返回打包后的字符串
        '''
        check_sum = self.version + self.attr + self.msg_type + self.task_id + self.data_len
        return struct.pack("!BBHIIH", self.version, self.attr, self.msg_type, self.task_id, self.data_len, check_sum)
    
    @staticmethod
    def check_attr(word, attr):
        return word & attr != 0

    @staticmethod
    def set_attr(word, attr):
        return word | attr

    def is_keepalive(self):
        '''
        @brief 是否为keep-alive的消息
        @return bool
        '''
        return CwxMsgHeader.check_attr(self.attr, CwxMsgHeader.ATTR_SYS_MSG) and\
                self.msg_type == CwxMsgHeader.TYPE_KEEPALIVE

    def is_keepalive_reply(self):
        '''
        @brief 是否为keep-alive的回复消息
        @return bool
        '''
        return CwxMsgHeader.check_attr(self.attr, CwxMsgHeader.ATTR_SYS_MSG) and\
                self.msg_type == CwxMsgHeader.TYPE_KEEPALIVE_REPLY

    def is_sys_msg(self):
        '''
        @brief 是否系统消息
        @return bool
        '''
        return CwxMsgHeader.check_attr(self.attr, CwxMsgHeader.ATTR_SYS_MSG)

    def set_sys_msg(self):
        '''
        @brief 设置系统消息标记
        @return void
        '''
        self.attr = CwxMsgHeader.set_attr(self.attr, CwxMsgHeader.ATTR_SYS_MSG)

    def set_keepalive(self):
        '''
        @brief 设置消息头为keep-alive信息
        @return void
        '''
        self.attr = CwxMsgHeader.set_attr(self.attr, CwxMsgHeader.ATTR_SYS_MSG)
        self.msg_type = CwxMsgHeader.TYPE_KEEPALIVE

    def set_keepalive_reply(self):
        '''
        @brief 设置消息头为keep-alive的reply信息
        @return void
        '''
        self.attr = CwxMsgHeader.set_attr(self.attr, CwxMsgHeader.ATTR_SYS_MSG)
        self.msg_type = CwxMsgHeader.TYPE_KEEPALIVE_REPLY

    def is_compressed(self):
        '''
        @brief 是否压缩数据包
        @return bool
        '''
        return CwxMsgHeader.check_attr(self.attr, CwxMsgHeader.ATTR_COMPRESS)
