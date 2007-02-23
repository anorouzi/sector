#ifndef __CONF_H__
#define __CONF_H__

#include <string>
#include <fstream>

using namespace std;

struct Param
{
   string m_strName;
   string m_strValue;
};

class ConfParser
{
public:
   int init(string path);
   void close();
   int getNextParam(Param& param);

private:
   char* getToken(char* str, string& token);

private:
   ifstream m_ConfFile;
};

class SECTORParam
{
public:
   int init(const string& path);

public:
   string m_strDataDir;		// DATADIR
   int m_iSECTORPort;		// SECTOR_PORT
   int m_iRouterPort;		// ROUTER_PORT
};

#endif
