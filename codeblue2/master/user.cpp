/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 06/21/2009
*****************************************************************************/

#include "user.h"

using namespace std;


int ActiveUser::deserialize(vector<string>& dirs, const string& buf)
{
   unsigned int s = 0;
   while (s < buf.length())
   {
      unsigned int t = buf.find(';', s);

      if (buf.c_str()[s] == '/')
         dirs.insert(dirs.end(), buf.substr(s, t - s));
      else
         dirs.insert(dirs.end(), "/" + buf.substr(s, t - s));
      s = t + 1;
   }

   return dirs.size();
}

bool ActiveUser::match(const string& path, int32_t rwx)
{
   // check read flag bit 1 and write flag bit 2
   rwx &= 3;

   if ((rwx & 1) != 0)
   {
      for (vector<string>::iterator i = m_vstrReadList.begin(); i != m_vstrReadList.end(); ++ i)
      {
         if ((path.length() >= i->length()) && (path.substr(0, i->length()) == *i) && ((path.length() == i->length()) || (path.c_str()[i->length()] == '/') || (*i == "/")))
         {
            rwx ^= 1;
            break;
         }
      }
   }

   if ((rwx & 2) != 0)
   {
      for (vector<string>::iterator i = m_vstrWriteList.begin(); i != m_vstrWriteList.end(); ++ i)
      {
         if ((path.length() >= i->length()) && (path.substr(0, i->length()) == *i) && ((path.length() == i->length()) || (path.c_str()[i->length()] == '/') || (*i == "/")))
         {
            rwx ^= 2;
            break;
         }
      }
   }

   return (rwx == 0);
}

int ActiveUser::serialize(char*& buf, int& size)
{
   buf = new char[65536];
   char* p = buf;
   *(int32_t*)p = m_strName.length() + 1;
   p += 4;
   strcpy(p, m_strName.c_str());
   p += m_strName.length() + 1;
   *(int32_t*)p = m_strIP.length() + 1;
   p += 4;
   strcpy(p, m_strIP.c_str());
   p += m_strIP.length() + 1;
   *(int32_t*)p = m_iPort;
   p += 4;
   *(int32_t*)p = m_iDataPort;
   p += 4;
   *(int32_t*)p = m_iKey;
   p += 4;
   memcpy(p, m_pcKey, 16);
   p += 16;
   memcpy(p, m_pcIV, 8);
   p += 8;
   *(int32_t*)p = m_vstrReadList.size();
   p += 4;
   for (vector<string>::iterator i = m_vstrReadList.begin(); i != m_vstrReadList.end(); ++ i)
   {
      *(int32_t*)p = i->length() + 1;
      p += 4;
      strcpy(p, i->c_str());
      p += i->length() + 1;
   }
   *(int32_t*)p = m_vstrWriteList.size();
   p += 4;
   for (vector<string>::iterator i = m_vstrWriteList.begin(); i != m_vstrWriteList.end(); ++ i)
   {
      *(int32_t*)p = i->length() + 1;
      p += 4;
      strcpy(p, i->c_str());
      p += i->length() + 1;
   }
   *(int32_t*)p = m_bExec;
   p += 4;

   size = p - buf;
   return size;
}

int ActiveUser::deserialize(const char* buf, const int& size)
{
   char* p = (char*)buf;
   m_strName = p + 4;
   p += 4 + m_strName.length() + 1;
   m_strIP = p + 4;
   p += 4 + m_strIP.length() + 1;
   m_iPort = *(int32_t*)p;
   p += 4;
   m_iDataPort = *(int32_t*)p;
   p += 4;
   m_iKey = *(int32_t*)p;
   p += 4;
   memcpy(m_pcKey, p, 16);
   p += 16;
   memcpy(m_pcIV, p, 8);
   p += 8;
   int num = *(int32_t*)p;
   p += 4;
   for (int i = 0; i < num; ++ i)
   {
      p += 4;
      m_vstrReadList.push_back(p);
      p += strlen(p) + 1;
   }
   num = *(int32_t*)p;
   p += 4;
   for (int i = 0; i < num; ++ i)
   {
      p += 4;
      m_vstrWriteList.push_back(p);
      p += strlen(p) + 1;
   }
   m_bExec = *(int32_t*)p;

   return size;
}
