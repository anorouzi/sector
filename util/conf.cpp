/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#include "conf.h"
#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>

using namespace std;
using namespace cb;

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
   //param format
   // NAME
   // < tab >value1
   // < tab >value2
   // < tab >...
   // < tab >valuen

   param.m_strName = "";
   param.m_vstrValue.clear();

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

      // no blanks or tabs in front of name line
      if ((' ' == buf[0]) || ('\t' == buf[0]))
         return -1;

      char* str = buf;
      string token = "";

      if (NULL == (str = getToken(str, token)))
         continue;
      param.m_strName = token;

      // scan param values
      while (!m_ConfFile.eof())
      {
         m_ConfFile.getline(buf, 1024);

         if ('\t' != buf[0])
            break;

         str = buf;
         if (NULL == (str = getToken(str, token)))
            return -1;

         param.m_vstrValue.insert(param.m_vstrValue.end(), token);
      }

      return 0;
   }

   return -1;
}

char* ConfParser::getToken(char* str, string& token)
{
   char* p = str;

   // skip blank spaces
   while ((' ' == *p) || ('\t' == *p))
      ++ p;

   // nothing here...
   if ('\0' == *p)
      return NULL;

   token = "";
   while ((' ' != *p) && ('\t' != *p) && ('\0' != *p))
   {
      token.append(1, *p);
      ++ p;
   }

   return p;
}

int SECTORParam::init(const string& path)
{
   m_strDataDir = "../data/";
   m_llMaxDataSize = 0;		
   m_iSECTORPort = 2237;	// CBFS
   m_iRouterPort = 24673;	// CHORD
   m_iDataPort = 8386;		// UDTM

   ConfParser parser;
   Param param;

   if (0 != parser.init(path))
   {
      cerr << "couldn't locate SETCOR configuration file. Please check " << path << endl;
      return -1;
   }

   while (0 == parser.getNextParam(param))
   {
      if (param.m_vstrValue.empty())
         continue;

      if ("DATADIR" == param.m_strName)
         m_strDataDir = param.m_vstrValue[0];
      else if ("MAXDATASIZE" == param.m_strName)
         m_llMaxDataSize = atoll(param.m_vstrValue[0].c_str());
      else if ("SECTOR_PORT" == param.m_strName)
         m_iSECTORPort = atoi(param.m_vstrValue[0].c_str());
      else if ("ROUTER_PORT" == param.m_strName)
         m_iRouterPort = atoi(param.m_vstrValue[0].c_str());
      else if ("DATA_PORT" == param.m_strName)
         m_iDataPort = atoi(param.m_vstrValue[0].c_str());
      else if ("WRITE_ALLOWED" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
            m_IPSec.addIP(*i);
      }
      else if ("MAX_SPE_MEM" == param.m_strName)
         m_iMaxSPEMem = atoi(param.m_vstrValue[0].c_str());
      else
         cerr << "unrecongnized system parameter: " << param.m_strName << endl;
   }

   parser.close();

   return 0;
}

IPSec::IPSec()
{
   m_vIPList.clear();
}

int IPSec::addIP(const string& ip)
{
   char buf[64];
   int i = 0;
   for (int n = ip.length(); i < n; ++ i)
   {
      if ('/' == ip.c_str()[i])
         break;

      buf[i] = ip.c_str()[i];
   }
   buf[i] = '\0';

   in_addr addr;
   if (inet_pton(AF_INET, buf, &addr) < 0)
      return -1;

   IPRange entry;
   entry.m_uiIP = addr.s_addr;
   entry.m_uiMask = 0xFFFFFFFF;

   if (i == ip.length())
   {
      m_vIPList.insert(m_vIPList.end(), entry);
      return 0;
   }

   if ('/' != ip.c_str()[i])
      return -1;
   ++ i;

   bool format = false;
   int j = 0;
   for (int n = ip.length(); i < n; ++ i, ++ j)
   {
      if ('.' == ip.c_str()[i])
         format = true;

      buf[j] = ip.c_str()[i];
   }
   buf[j] = '\0';

   if (format)
   {
      //255.255.255.0
      if (inet_pton(AF_INET, buf, &addr) < 0)
         return -1;
      entry.m_uiMask = addr.s_addr;
   }
   else
   {
      char* p;
      unsigned int bit = strtol(buf, &p, 10);

      if ((p == buf) || (bit > 32) || (bit < 0))
         return -1;

      if (bit < 32)
         entry.m_uiMask = ((unsigned int)1 << bit) - 1;
   }

   m_vIPList.insert(m_vIPList.end(), entry);

   return 0;
}

bool IPSec::checkIP(const string& ip)
{
   in_addr addr;
   if (inet_pton(AF_INET, ip.c_str(), &addr) < 0)
      return false;

   for (vector<IPRange>::iterator i = m_vIPList.begin(); i != m_vIPList.end(); ++ i)
   {
      if ((addr.s_addr & i->m_uiMask) == (i->m_uiIP & i->m_uiMask))
         return true;
   }

   return false;
}
