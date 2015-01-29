#include "CwxMqConnector.h"

int CwxMqConnector::complete(set<int>& fds, CwxTimeouter* timeout) {
  fd_set handle_set;
  FD_ZERO(&handle_set);
  int select_width = 0;
  int result = 0;
  set<int>::iterator iter;
  int fd = 0;
  int sock_err = 0;
  socklen_t sock_err_len = sizeof(sock_err);
  int sockopt_ret = 0;
  while (fds.size()) {
    iter = fds.begin();
    while (iter != fds.end()) {
      FD_SET(*iter, &handle_set);
      if (*iter > select_width)
        select_width = *iter;
      iter++;
    }
    select_width++;
    result = CwxSocket::select(select_width, NULL, &handle_set, NULL, timeout,
      true);
    if (0 > result) {
      return -1;
    } else if (0 == result) {
      errno = ETIME;
      return -1;
    }
    iter = fds.begin();
    while (iter != fds.end()) {
      if (FD_ISSET(*iter, &handle_set)) {
        fd = *iter;
        fds.erase(iter);
        iter = fds.upper_bound(fd);
        sock_err_len = sizeof(sock_err);
        sockopt_ret = ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &sock_err,
          &sock_err_len);
        if ((sockopt_ret < 0) || sock_err) {
          errno = sock_err;
          return -1;
        }
        CwxSockStream::setNonblock(fd, false);
      } else {
        iter++;
      }
    }
  }
  return 0;
}

int CwxMqConnector::connect(CwxINetAddr const& addr,
                            CWX_UINT16 unConnNum,
                            int* fd,
                            CwxTimeouter* timeout,
                            bool reuse_addr,
                            CWX_NET_SOCKET_ATTR_FUNC fn, ///<socket设置的function
                            void* fnArg)
{
  CWX_UINT16 i;
  bool bRet = false;
  CwxSockStream stream;
  set<int> fdset;
  ///初始化连接
  for (i = 0; i < unConnNum; i++)
    fd[i] = -1;
  do {
    ///建立socket
    for (i = 0; i < unConnNum; i++) {
      if (stream.open(addr.getType(), SOCK_STREAM, 0, reuse_addr) == -1) {
        break;
      }
      fd[i] = stream.getHandle();
      stream.setHandle(CWX_INVALID_HANDLE);
      // 设置keepalive
      if (0 != CwxSocket::setKeepalive(fd[i],
        true,
        CWX_APP_DEF_KEEPALIVE_IDLE,
        CWX_APP_DEF_KEEPALIVE_INTERNAL,
        CWX_APP_DEF_KEEPALIVE_COUNT)) break;
      // Enable non-blocking, if required.
      if ((timeout != 0) && (CwxSockStream::setNonblock(fd[i], true) == -1)) {
        break;
      }
      if (fn && (0 != fn(fd[i], fnArg))) {
        break;
      }
      int result = ::connect(fd[i],
        reinterpret_cast<sockaddr *>(addr.getAddr()), addr.getSize());

      if (result == -1 && timeout != 0) {
        // Check whether the connection is in progress.
        if (errno == EINPROGRESS || errno == EWOULDBLOCK) {
          fdset.insert(fd[i]);
        }
      }
      // EISCONN is treated specially since this routine may be used to
      // check if we are already connected.
      if (result != -1 || errno == EISCONN) {
        // Start out with non-blocking disabled on the new_stream.
        result = CwxSockStream::setNonblock(fd[i], false);
        if (result == -1) break;
      } else if (!(errno == EWOULDBLOCK || errno == EINPROGRESS)) {
        break;
      }
    }
    if (i < unConnNum) break;
    bRet = true;
  } while (0);

  if (bRet) {
    if (fdset.size()) {
      if (0 != complete(fdset, timeout)) {
        bRet = false;
      }
    }
  }
  if (!bRet) {
    CwxErrGuard guard;
    for (i = 0; i < unConnNum; i++) {
      if (fd[i] != -1) {
        ::close(fd[i]);
        fd[i] = -1;
      }
    }
    return -1;
  }
  return 0;
}

