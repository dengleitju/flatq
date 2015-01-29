#include "CwxMqTss.h"

///构造函数
CwxMqTss::~CwxMqTss() {
  if (m_pReader) delete m_pReader;
  if (m_pItemReader) delete m_pItemReader;
  if (m_pWriter) delete m_pWriter;
  if (m_pItemWriter) delete m_pItemWriter;
  if (m_szDataBuf) delete[] m_szDataBuf;
}

int CwxMqTss::init() {
  m_pReader = new CwxPackageReader(false);
  m_pItemReader = new CwxPackageReader(false);
  m_pWriter = new CwxPackageWriter(MAX_PACKAGE_SIZE);
  m_pItemWriter = new CwxPackageWriter(MAX_PACKAGE_SIZE);
  m_szDataBuf = new char[MAX_PACKAGE_SIZE];
  m_uiDataBufLen = MAX_PACKAGE_SIZE;
  return 0;
}
