##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE="20141031"


bin_PROGRAMS = flatq_subproxy
flatq_subproxy_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)" -W -Wall  -Wno-deprecated
flatq_subproxy_SOURCES = CwxSubProxyApp.cpp\
		CwxSubProxyConfig.cpp\
		CwxSubProxySyncHandler.cpp\
		CwxSubProxyZkHandler.cpp\
		cwx_subscribe_proxy.cpp\
		../common/CwxMqDef.cpp\
                ../common/CwxMqPoco.cpp\
		../common/CwxSidLogFile.cpp\
		../common/CwxMqTss.cpp\
                ../common/CwxBinLogMgr.cpp\
                ../common/CwxMqConnector.cpp\
                ../zk/CwxZkAdaptor.cc\
                ../zk/CwxZkLocker.cc 
flatq_subproxy_LDADD=
flatq_subproxy_LDFLAGS =-L@zk_dir@/lib -L@cwinux_dir@/lib -L@openssl_dir@/lib
LIBS=-lcwxapp -lcwxnet -lcwxcmn -lzookeeper_mt -lssl -lpthread -ldl -lrt -lz
INCLUDES=-I@cwinux_dir@/include/app -I@cwinux_dir@/include/cmn -I@cwinux_dir@/include/net  -I@zk_dir@/ -I@openssl_dir@/include -I../common  -I../zk

## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
