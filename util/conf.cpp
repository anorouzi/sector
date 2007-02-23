#include "conf.h"
#include <iostream>
using namespace std;

int ConfParser::init(string path)
{
   m_ConfFile.open(path.c_str());

   if (m_ConfFile.bad())
      return -1;

   return 0;
}

void ConfParser::close()
{
   m_ConfFile.close();
}

int ConfParser::getNextParam(Param& param)
{
   //param format: name = value

   while (!m_ConfFile.eof())
   {
      char buf[1024];
      string name;

      m_ConfFile.getline(buf, 1024);

      // skip blank lines
      if (0 == strlen(buf))
         continue;

      // skip comments
      if ('#' == buf[0])
         continue;

      char* str = buf;
      string token = "";

      if (NULL == (str = getToken(str, token)))
         continue;
      name = token;

      if (NULL == (str = getToken(str, token)))
         continue;

      if ('=' != token[0])
         continue;

      if (NULL == (str = getToken(str, token)))
         continue;

      param.m_strName = name;
      param.m_strValue = token;

      return 0;
   }

   return -1;
}

char* ConfParser::getToken(char* str, string& token)
{
   char* p = str;

   // skip blank spaces
   while (' ' == *p)
      ++ p;

   // nothing here...
   if ('\0' == *p)
      return NULL;

   token = "";
   while ((' ' != *p) && ('\0' != *p))
   {
      token.append(1, *p);
      ++ p;
   }

   return p;
}

int SECTORParam::init(const string& path)
{
   m_strDataDir = "../data/";
   m_iSECTORPort = 2237;
   m_iRouterPort = 24673;

   ConfParser parser;
   Param param;

   if (0 != parser.init(path))
   {
      cout << "couldn't locate SETCOR configuration file. Please check " << path << endl;
      return -1;
   }

   while (0 == parser.getNextParam(param))
   {
      if ("DATADIR" == param.m_strName)
         m_strDataDir = param.m_strValue;
      else if ("SECTOR_PORT" == param.m_strName)
         m_iSECTORPort = atoi(param.m_strValue.c_str());
      else if ("ROUTER_PORT" == param.m_strName)
         m_iRouterPort = atoi(param.m_strValue.c_str());
   }

   parser.close();

   return 0;
}