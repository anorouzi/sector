/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/


This file is part of Sector Client.

The Sector Client is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

The Sector Client is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 01/22/2009
*****************************************************************************/

#include <util.h>
#include <cstring>
#include <cstdlib>
#include <iostream>

using namespace std;

bool WildCard::isWildCard(const string& path)
{
   if (path.find('*') != string::npos)
      return true;

   if (path.find('?') != string::npos)
      return true;

   return false;
}

bool WildCard::match(const string& card, const string& path)
{
   const char* p = card.c_str();
   const char* q = path.c_str();

   unsigned int i = 0;
   unsigned int j = 0;
   while ((i < card.length()) && (j < path.length()))
   {
      switch (p[i])
      {
      case '*':
         if (i == card.length() - 1)
            return true;

         while (p[i] == '*')
            ++ i;

         for (; j < path.length(); ++ j)
         {
            if (((q[j] == p[i]) || (p[i] == '?') ) && match(p + i, q + j))
               return true;
         }

         return false;

      case '?':
         break;

      default:
         if (p[i] != q[j])
            return false;
      }

      ++ i;
      ++ j;
   }

   if ((i != card.length()) || (j != path.length()))
      return false;

   return true;
}


int Session::loadInfo(const string& conf)
{
   m_ClientConf.init(conf);

   if (m_ClientConf.m_strMasterIP == "")
   {
      cout << "please input the master address (e.g., 123.123.123.123:1234): ";
      string addr;
      cin >> addr;

      char buf[128];
      strcpy(buf, addr.c_str());

      unsigned int i = 0;
      for (; i < strlen(buf); ++ i)
      {
         if (buf[i] == ':')
            break;
      }

      buf[i] = '\0';
      m_ClientConf.m_strMasterIP = buf;
      m_ClientConf.m_iMasterPort = atoi(buf + i + 1);
   }

   if (m_ClientConf.m_strUserName == "")
   {
      cout << "please input the user name: ";
      cin >> m_ClientConf.m_strUserName;
   }

   if (m_ClientConf.m_strPassword == "")
   {
      cout << "please input the password: ";
      cin >> m_ClientConf.m_strPassword;
   }

   if (m_ClientConf.m_strCertificate == "")
   {
      cout << "please specify the location of the master certificate: ";
      cin >> m_ClientConf.m_strCertificate;
   }

   //TODO: if m_strCert is relative dir, use getcwd to change it into absolute dir

   return 1;
}
