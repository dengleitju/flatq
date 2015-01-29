#coding:utf-8

from CwxMsgHeader import *
from CwxPackage import *
import binascii
import struct
import zlib
import hashlib
from CwxMqError import *

class CwxMqPoco:

    #协议的消息类型定义
    #RECV服务类型的消息类型定义
    MSG_TYPE_RECV_DATA = 1 #<数据提交消息
    MSG_TYPE_RECV_DATA_REPLY = 2 #<数据提交消息的回复
    #分发的消息类型定义
    MSG_TYPE_SYNC_OUTER_REPORT = 3 #<同步SID点报告消息类型
    MSG_TYPE_SYNC_REPORT_REPLY = 4 #<失败返回
    MSG_TYPE_SYNC_DATA = 7
    MSG_TYPE_SYNC_DATA_REPLY = 8
    MSG_TYPE_SYNC_DATA_CHUNK = 9
    MSG_TYPE_SYNC_DATA_CHUNK_REPLY = 10
    #错误消息
    MSG_TYPE_SYNC_ERR = 105
    #proxy发送消息
    MSG_TYPE_PROXY_RECV_DATA = 20
    MSG_TYPE_PROXY_RECV_DATA_REPLY = 21
    #proxy订阅消息
    MSG_TYPE_PROXY_REPORT = 22 #<读代理同步SID点报告消息类型
    MSG_TYPE_PROXY_SYNC_DATA = 24 #同步数据数据
    MSG_TYPE_PROXY_SYNC_DATA_REPLY = 25 #同步数据返回
    MSG_TYPE_PROXY_SYNC_CHUNK_DATA = 26 #同步chunk数据
    MSG_TYPE_PROXY_SYNC_CHUNK_DATA_REPLY = 27 #<同步chunk数据返回
    #协议的key定义
    KEY_DATA = "d"
    KEY_RET = "ret"
    KEY_SID = "sid"
    KEY_ERR = "err"
    KEY_TIMESTAMP = "t"
    KEY_CHUNK = "chunk"
    KEY_M = "m"
    KEY_ZIP = "zip"
    KEY_DELAY = "delay"
    KEY_TOPIC = "topic"
    KEY_STATE = "state"
    KEY_SESSION = "session"
    KEY_SOURCE = "source"
    KEY_GROUP = "g"
    
    def __init__(self):
        self.header = CwxMsgHeader()
        self.package = CwxPackage()
    
    def _reset(self):
        self.package.__init__()
        self.header.__init__()

    def __str__(self):
        package_str = str(self.package)
        if CwxMsgHeader.check_attr(self.header.attr, CwxMsgHeader.ATTR_COMPRESS):
            package_str = zlib.compress(package_str)
        self.header.data_len = len(package_str)
        return str(self.header) + package_str

    def parse_header(self, msg_header):
        '''
        @brief 解析消息头，存入self.header中
        @return None
        '''
        if len(msg_header) < CwxMsgHeader.HEAD_LEN:
            print CwxMsgHeader.HEAD_LEN, '  ', len(msg_header)
            raise BadPackageError()
        self.header.__init__(msg_header[:CwxMsgHeader.HEAD_LEN])
          
    def pack_mq(self, task_id, data, topic, group, zip=False):
        '''
        @brief 形成mq的一个消息包
        @param [in] task_id task-id,回复的时候会返回。
        @param [in] data msg的data。
        @param [in] group msg的group。
        @param [in] user 接收mq的user，若为空，则表示没有用户。
        @param [in] passwd 接收mq的passwd，若为空，则表示没有口令。
        @param [in] sign  接收的mq的签名类型，若为空，则表示不签名。
        @param [in] zip  是否对数据压缩.
        @return 生成的数据包
        '''
        self._reset()
        self.package.data[CwxMqPoco.KEY_DATA] = data
        self.package.data[CwxMqPoco.KEY_TOPIC] = topic
        if group != None:
          self.package.data[CwxMqPoco.KEY_GROUP] = group
        if zip:
            self.header.attr = CwxMsgHeader.ATTR_COMPRESS

        self.header.version = 0
        self.header.task_id = task_id
        self.header.msg_type = CwxMqPoco.MSG_TYPE_PROXY_RECV_DATA

        return str(self)
        
    def parse_package(self, msg):
        self.package.__init__()
        self.package = CwxPackage(msg)
                    
    def parse_mq_reply(self, msg):
        '''
        @brief 解析mq的一个reply消息包
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return OrderedDict({ret, sid, err)}
            ret 返回msg的ret。
            sid 返回msg的sid。
            err 返回msg的err-msg。
        '''
        self.parse_package(msg)
        ret = self.package.get_key_int(CwxMqPoco.KEY_RET)
        if ret == None:
            raise CwxMqError(CwxMqError.NO_RET, 
                "No key[%s] in recv page." % CwxMqPoco.KEY_RET)
        sid = 0
        if ret != CwxMqError.SUCCESS:
            err = self.package.get_key(CwxMqPoco.KEY_ERR)
            if err == None:
                raise CwxMqError(CwxMqError.NO_ERR,
                    "No key[%s] in recv page." % CwxMqPoco.KEY_ERR)
        else:
            err = ""
            sid = self.package.get_key_int(CwxMqPoco.KEY_SID)
            if sid == None:
              raise CwxMqError(CwxMqError.NO_SID,"No key[%s] in recv page." % CwxMqPoco.KEY_SID)

        return OrderedDict((("ret",ret), ("sid",sid), ("err",err)))
    
    def parse_err(self, msg):
      '''
      @brief 解析错误信息
      return OrderedDict({ret, err})
      '''
      self.parse_package(msg)
      ret = self.package.get_key_int(CwxMqPoco.KEY_RET)
      if ret == None:
        raise CwxMqError(CwxMqError.NO_RET, "No key[%s] in recv page." % CwxMqPoco.KEY_RET)
      err = self.package.get_key(CwxMqPoco.KEY_ERR)
      if err == None:
        raise CwxMqError(CwxMqError.NO_ERR,"No key[%s] in recv page." % CwxMqPoco.KEY_ERR)
      return OrderedDict((("ret",ret), ("err",err)))
      

    def pack_sync_report(self, task_id, sid=None, chunk=0,group=None, topic=None, source=None, zip=False):
        '''
        @brief pack mq的report消息包
        @param [in] task_id task-id。
        @param [in] sid 同步的sid。None表示从当前binlog开始接收
        @param [in] chunk chunk的大小，若是0表示不支持chunk，单位为kbyte。
        @param [in] zip  接收的mq是否压缩。
        @return 生成的数据包
        '''
        self._reset()
        if sid != None:
            self.package.data[CwxMqPoco.KEY_SID] = sid
        if chunk:
            self.package.data[CwxMqPoco.KEY_CHUNK] = chunk
        if group:
            self.package.data[CwxMqPoco.KEY_GROUP] = group
        if topic:
            self.package.data[CwxMqPoco.KEY_TOPIC] = topic
        if source:
            self.package.data[CwxMqPoco.KEY_SOURCE] = source
        if zip:
            self.package.data[CwxMqPoco.KEY_ZIP] = 1

        self.header.msg_type = CwxMqPoco.MSG_TYPE_PROXY_REPORT
        self.header.task_id = task_id

        return str(self)

    def parse_sync_report_reply(self, msg):
        '''
        @brief parse mq的report失败时的reply消息包
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return OrderedDict({ret, sid, err})
            ret report失败的错误代码。
            sid report的sid。
            err report失败的原因。
        '''
        self.parse_package(msg)
        ret = self.package.get_key_int(CwxMqPoco.KEY_SESSION)
        if ret == None:
            raise CwxMqError(CwxMqError.NO_RET, 
                "No key[%s] in recv page." % CwxMqPoco.KEY_SESSION)
        return "session" + str(ret)

    def parse_sync_data(self, msg):
        '''
        @brief parse mq的sync msg的消息包
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return [OrderedDict({sid, timestamp, data, group}),...]
            返回的list中按顺序包含每条消息。非chunk模式只包含一个消息。
            sid 消息的sid。
            timestamp 消息接收时的时间。
            data 消息的data。
            group 消息的group。
        '''
        self.parse_package(msg)
        res = []
        sid = self.package.get_key_int(CwxMqPoco.KEY_SID)
        if sid == None:
          raise CwxMqError(CwxMqError.No_SID, "No key[sid] in recv page.")
        time_stamp = self.package.get_key_int(CwxMqPoco.KEY_TIMESTAMP)
        if time_stamp == None:
          raise CwxMqError(CwxMqError.NO_SID, "No key[t] in recv_page.")
        topic = self.package.get_key(CwxMqPoco.KEY_TOPIC)
        if topic == None:
          raise CwxMqError(CwxMqError.NO_SID, "No key[topic] in recv_page.")
        data = self.package.get_key(CwxMqPoco.KEY_DATA)
        if data == None:
          raise CwxMqError(CwxMqError.NO_SID, "No key[d] in recv_page.")
        res.append(OrderedDict((("sid",sid), ("timestamp",time_stamp), ("topic",topic),
                ("data",data))))
        return res

    def pack_sync_data_reply(self, task_id, seq, sid):
        '''
        @brief pack mq的sync msg的消息包的回复
        @param [in] task_id task-id。
        @param [in] sid 消息的sid。
        @return 生成的数据包
        '''
        self._reset()
        self.header.msg_type = CwxMqPoco.MSG_TYPE_PROXY_SYNC_DATA_REPLY
        self.header.task_id = task_id
        self.header.data_len =16 
        return str(self.header) + seq + sid
    
    def parse_sync_mult_data(self, msg):
      '''
      @brief 解析chunk分发数据
      '''
      self.parse_package(msg)
      packages = []
      if len(self.package.data) == 1 and CwxMqPoco.KEY_M in self.package.data:
        msgs = self.package.get_key(CwxMqPoco.KEY_M)
        if isinstance(msgs, list):
          for m in self.package.get_key(CwxMqPoco.KEY_M):
            p = CwxPackage()
            p.data = m
            packages.append(p)
        else:
          p = CwxPackage()
          p.data = msgs
          packages.append(p)
      else:
        packages.append(self.package)
    
      res = []
      for p in packages:
        sid = p.get_key_int(CwxMqPoco.KEY_SID)
        if sid == None:
          raise CwxMqError(CwxMqError.NO_SID, "No key[sid] in recv page.")
        time_stamp = p.get_key_int(CwxMqPoco.KEY_TIMESTAMP)
        if time_stamp == None:
          raise CwxMqError(CwxMqError.NO_SID, "No key[t] in recv_page.")
        topic = p.get_key(CwxMqPoco.KEY_TOPIC)
        if topic == None:
          raise CwxMqError(CwxMqError.NO_SID, "No key[topic] in recv_page.")
        data = p.get_key(CwxMqPoco.KEY_DATA)
        if data == None:
          raise CwxMqError(CwxMqError.NO_SID, "No key[d] in recv_page.")
        res.append(OrderedDict((("sid",sid), ("timestamp",time_stamp), ("topic",topic),
                ("data",data))))
      return res
