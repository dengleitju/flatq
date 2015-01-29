#coding:utf-8
from cwxmq.CwxMqPoco import CwxMqPoco
import socket
from cwxmq.CwxMsgHeader import CwxMsgHeader
import zlib
import binascii
import struct

host = '127.0.0.1'


send_info = {"port":9901, "data":{"a":1,"b":2}, "zip":True}

sync_info = {"port":9903}

send_conn = socket.create_connection((host, send_info["port"]))

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

def test_send_data(i):
    conn = send_conn
    pack = poco.pack_mq(0, "data"+str(i), "topic1", send_info["zip"])
    conn.sendall(pack)
    msg = recv_msg(conn)
    res = poco.parse_mq_reply(msg)
    print "send data reply:", res
    print
    return res


def test_sync_data():
    print "sync data"
    conn = socket.create_connection((host, sync_info["port"]))
    conn.settimeout(2)
    pack = poco.pack_sync_report(0, 2, 1, "topic1", "topic1_source", True)
    conn.sendall(pack)
    i = 0
    while True:
        i += 1
        try:
            msg = recv_msg(conn)
        except socket.timeout:
            print "timeout"
            break
        if poco.header.msg_type == CwxMqPoco.MSG_TYPE_SYNC_DATA:
            seq = msg[0:8]
            msg = msg[8:]
            msg = zlib.decompress(msg)
            print "-------------------------------------"
            print "received sync data, seq:", getSeq(seq)
            res = poco.parse_sync_data(msg)
            print res
            pack = poco.pack_sync_data_reply(0, seq)
            conn.sendall(pack)
        elif poco.header.msg_type == CwxMqPoco.MSG_TYPE_SYNC_DATA_CHUNK:
          seq = msg[0:8]
          msg = msg[8:]
          msg = zlib.decompress(msg)
          print "----------------------------------------"
          print "recieve mult syn data, seq", getSeq(seq)
          res = poco.parse_sync_mult_data(msg)
          for item in res:
            print item
          pack = poco.pack_sync_data_reply(0, seq)
          conn.sendall(pack)
        elif poco.header.msg_type == CwxMqPoco.MSG_TYPE_SYNC_REPORT_REPLY:
          print "----------------------------------------"
          print "sync report reply:", poco.parse_sync_report_reply(msg)
        else:
            print "----------------------------------------"
            assert poco.header.msg_type == CwxMqPoco.MSG_TYPE_SYNC_ERR
            print "recv error"
            print poco.parse_err(msg)
            return 
            

    print

if __name__ == "__main__":
  #send data to mq
  '''for i in xrange(10):
    test_send_data(i)'''
  #sync data from mq
  test_sync_data() 