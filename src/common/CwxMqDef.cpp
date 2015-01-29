#include "CwxMqDef.h"
#include "CwxMqPoco.h"

bool mqParseHostPort(string const& strHostPort, CwxHostInfo& host) {
  if ((strHostPort.find(':') == string::npos) || (0 == strHostPort.find(':')))
    return false;
  host.setHostName(strHostPort.substr(0, strHostPort.find(':')));
  host.setPort(
    strtoul(strHostPort.substr(strHostPort.find(':') + 1).c_str(), NULL, 10));
  return true;
}
