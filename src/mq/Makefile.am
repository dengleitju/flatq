##  Process this file with automake to create Makefile.in

includedir =
pkgconfigdir =

BUILD_DATE="20141031"


bin_PROGRAMS = flatq
flatq_CPPFLAGS = -D_BUILD_DATE="$(BUILD_DATE)" -W -Wall  -Wno-deprecated
flatq_SOURCES = CwxMqApp.cpp\
		CwxMqInnerDispHandler.cpp\
		CwxMqConfig.cpp\
		CwxMqMasterHandler.cpp\
		CwxMqOuterDispHandler.cpp\
		CwxMqRecvHandler.cpp\
		CwxMqZkHandler.cpp\
		cwx_mq.cpp\
		../common/CwxMqDef.cpp\
                ../common/CwxMqPoco.cpp\
		../common/CwxSidLogFile.cpp\
		../common/CwxMqTss.cpp\
                ../common/CwxBinLogMgr.cpp\
                ../common/CwxMqConnector.cpp\
                ../zk/CwxZkAdaptor.cc\
                ../zk/CwxZkLocker.cc\
flatq_LDADD=
flatq_LDFLAGS =-L@zk_dir@/lib -L@cwinux_dir@/lib -L@openssl_dir@/lib
LIBS=-lcwxapp -lcwxnet -lcwxcmn -lzookeeper_mt -lssl -lpthread -ldl -lrt -lz
INCLUDES=-I@cwinux_dir@/include/app -I@cwinux_dir@/include/cmn -I@cwinux_dir@/include/net  -I@zk_dir@/ -I@openssl_dir@/include -I../common -I../zk

## Clean up template repositories, etc.
clean-local:
	-rm -f *~ *.bak *.rpo *.sym lib*.*_pure_* core core.*
