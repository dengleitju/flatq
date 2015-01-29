#coding:utf-8


import struct
from CwxMqError import BadPackageError

class OrderedDict:
    '''
    有序字典
    '''
    def __init__(self, org_dict=None):
        if org_dict:
            if isinstance(org_dict, dict):
                self._data = org_dict
                self._keys = org_dict.keys()
            else:
                if isinstance(org_dict, (list,tuple)):
                    self._data = dict(org_dict)
                    self._keys = [i[0] for i in org_dict]
                else:
                    self.__init__()
        else:
            self._data = {}
            self._keys = []
    
    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        return self._data[key]
    
    def __setitem__(self, key, value):
        if key not in self._data:
            self._keys.append(key)
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]
        self._keys.remove(key)
    
    def __iter__(self):
        return self._keys.__iter__()
    
    def __reversed__(self):
        return self._keys.__reversed__()
    
    def __contains__(self, item):
        return item in self._keys
        
    def keys(self):
        return self._keys

    def items(self):
        return [(k, self._data[k]) for k in self._keys]

    def values(self):
        return [self._data[k] for k in self._keys]
    
    def __repr__(self):
        return "{"+", ".join([repr(k)+":"+repr(v) for k,v in self.items()])+"}"

    def __str__(self):
        return repr(self)
    
    def get(self, key, default=None):
        return self._data.get(key, default)

    def copy(self):
        d = OrderedDict()
        d._data = self._data.copy()
        d._keys = self._keys[:]
        return d

class CwxPackage:
    '''
    消息包类，包数据以OrderedDict形式保存在self.data中，可直接读取修改。
    value可以为tuple或list，但不能嵌套两层，但可以嵌套dict或OrderedDict。
    尽量避免把tuple或list作为value值，因为其他平台接口可能不支持。
    '''
    #Key/value标志位
    KV_BIT = 31
    #最大的kv长度
    MAX_KV_LEN = 0x7FFFFFFF

    def __init__(self, msg=None): 
        self.data = CwxPackage.unpack(msg)
    
    @staticmethod
    def unpack(msg):
        '''
        将消息体解包为OrderedDict字典
        '''
        res = OrderedDict()
        while msg:
            head = struct.unpack("!I", msg[:4])[0]
            is_key_value = (head & (1 << CwxPackage.KV_BIT)) > 0
            key_value_len = head & CwxPackage.MAX_KV_LEN
            if (key_value_len > len(msg)):
                raise BadPackageError()
            key_len = struct.unpack("!H", msg[4:6])[0]
            key = msg[6: 6 + key_len]
            if key_len > key_value_len or msg[6 + key_len] != "\0":
                raise BadPackageError()
            data_len = key_value_len - key_len - 8
            value = msg[6 + key_len + 1 : 6 + key_len + 1 + data_len]
            if is_key_value:
                value = CwxPackage.unpack(value)
            if key in res:
                if not isinstance(res[key], list):
                    res[key] = [res[key],]
                res[key].append(value)
            else:
                res[key] = value
            msg = msg[key_value_len:]

        return res
    
    @staticmethod
    def pack(data):
        '''
        将OrderedDict字典打包为消息体
        '''
        res = ""
        i = 0
        j = 0
        item = data.items()
        while i < len(item):
            (key, value) = item[i]
            s = ""
            key_str = str(key)
            if isinstance(value, (list, tuple)):
                if j >= len(value):
                    j = 0
                    i += 1
                    continue
                else:
                    value = value[j]
                    j += 1
            else:
                i += 1

            if isinstance(value, (dict, OrderedDict)):
                is_key_value = True
                value_str = CwxPackage.pack(value)
            else:
                is_key_value = False
                value_str = str(value)
            
            head = 8 + len(key_str) + len(value_str)
            if is_key_value:
                head |= (1 << CwxPackage.KV_BIT)
            res += struct.pack("!IH", head, len(key_str)) + key_str + "\0" + value_str + "\0"
        return res
   
    def get_key(self, key):
        return self.data.get(key)

    def get_key_int(self, key):
        if key in self.data:
            return int(self.data[key])
        else:
            return None

    def __str__(self):
        return CwxPackage.pack(self.data)
