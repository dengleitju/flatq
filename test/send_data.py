#coding:utf-8
from cwxmq.CwxMqPoco import CwxMqPoco
import socket
from cwxmq.CwxMsgHeader import CwxMsgHeader
import zlib
import binascii
import struct
import time

host = '127.0.0.1'


send_info = {"port":9990,  "zip":False}


send_conn = socket.create_connection((host, send_info["port"]))

poco = CwxMqPoco()

def recv_msg(conn):
    poco.parse_header(conn.recv(CwxMsgHeader.HEAD_LEN))
    #print "recv msg:",poco.header.data_len
    msg = conn.recv(poco.header.data_len)
    if poco.header.is_compressed():
        msg = zlib.decompress(msg)
    return msg

def getSeq(seq):
  ret = struct.unpack("!II", seq)
  low = ret[0]
  high = ret[1]
  low <<=32
  return low + high

def test_send_data(i):
    conn = send_conn
    pack = poco.pack_mq(i, "data"+str(i), "topic1", None, send_info["zip"])
    conn.sendall(pack)
    #print "task_id:",i,"..."
    msg = recv_msg(conn)
    res = poco.parse_mq_reply(msg)
    print "send data reply:", res, " data",i 
    #print
    return res
    if len(res['err'] > 1):
      exit(0)



if __name__ == "__main__":
  #send data to mq
  start_time = time.time()
  sum_num =10000000 
  for i in xrange(sum_num):
    test_send_data(i)
  end_time = time.time()
  print end_time - start_time, sum_num/(end_time - start_time)
