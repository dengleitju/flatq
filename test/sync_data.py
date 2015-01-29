#coding:utf-8
from cwxmq.CwxMqPoco import CwxMqPoco
import socket
from cwxmq.CwxMsgHeader import CwxMsgHeader
import zlib
import binascii
import struct

host = '127.0.0.1'



sync_info = {"port":9930}


poco = CwxMqPoco()

def recv_msg(conn):
    poco.parse_header(conn.recv(CwxMsgHeader.HEAD_LEN))
    msg = conn.recv(poco.header.data_len)
    '''if poco.header.is_compressed():
        msg = zlib.decompress(msg)'''
    return msg

def getSeq(seq):
  ret = struct.unpack("!II", seq)
  low = ret[0]
  high = ret[1]
  low <<=32
  return low + high

def setSid(sid):
  heigh = sid >> 32
  heigh = struct.pack("!I", heigh)
  low = sid & 0xFFFFFFFF
  low = struct.pack("!I", low)
  return heigh + low

def test_sync_data():
    print "sync data"
    conn = socket.create_connection((host, sync_info["port"]))
    #conn.settimeout(2)
    pack = poco.pack_sync_report(0, 0, 1, "group1", "topic1", "source1", True)
    conn.sendall(pack)
    i = 0
    while True:
        print "recv........"
        i += 1
        try:
            msg = recv_msg(conn)
        except socket.timeout:
            print "timeout"
            break
        print "recv msg :", poco.header.msg_type, " zip:",poco.header.is_compressed()
        print " task_id:", poco.header.task_id
        if poco.header.msg_type == CwxMqPoco.MSG_TYPE_PROXY_SYNC_DATA:
            seq = msg[0:8]
            msg = msg[8:]
            if poco.header.is_compressed():
              msg = zlib.decompress(msg)
            print "-------------------------------------"
            print "received sync data, seq:", getSeq(seq)
            res = poco.parse_sync_data(msg)
            print res
            sid = 0
            for item in res:
              print item
              if item['sid'] > sid:
                sid = item['sid']
            pack = poco.pack_sync_data_reply(poco.header.task_id, seq, setSid(sid))
            conn.sendall(pack)
        elif poco.header.msg_type == CwxMqPoco.MSG_TYPE_PROXY_SYNC_CHUNK_DATA:
          seq = msg[0:8]
          msg = msg[8:]
          if poco.header.is_compressed():
            msg = zlib.decompress(msg)
          print "----------------------------------------"
          print "recieve mult syn data, seq", getSeq(seq)
          res = poco.parse_sync_mult_data(msg)
          sid = 0
          for item in res:
            print item
            if item['sid'] > sid:
               sid = item['sid']
          print "sid:", sid
          pack = poco.pack_sync_data_reply(poco.header.task_id, seq, setSid(sid))
          print len(pack)
          conn.sendall(pack)
        else:
            print "----------------------------------------"
            assert poco.header.msg_type == CwxMqPoco.MSG_TYPE_SYNC_ERR
            print "recv error"
            print poco.parse_err(msg)
            return 
            

    print

if __name__ == "__main__":
  #sync data from mq
  test_sync_data() 
