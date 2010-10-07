/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 10/06/2010
*****************************************************************************/


#include <fstream>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <sector.h>

using namespace std;

SNode::SNode():
m_strName(""),
m_bIsDir(false),
m_llTimeStamp(0),
m_llSize(0),
m_strChecksum(""),
m_iReplicaNum(1),
m_iReplicaDist(65536)
{
   m_sLocation.clear();
   m_mDirectory.clear();
}

SNode::~SNode()
{
}

int SNode::serialize(char*& buf) const
{
   int namelen = m_strName.length();
   try
   {
      buf = new char[namelen + 256 + m_sLocation.size() * 256];
   }
   catch (...)
   {
      return -1;
   }

   sprintf(buf, "%d,%s,%d,%lld,%lld", namelen, m_strName.c_str(), m_bIsDir, (long long int)m_llTimeStamp, (long long int)m_llSize);
   char* p = buf + strlen(buf);
   for (set<Address, AddrComp>::const_iterator i = m_sLocation.begin(); i != m_sLocation.end(); ++ i)
   {
      sprintf(p, ",%s,%d", i->m_strIP.c_str(), i->m_iPort);
      p = p + strlen(p);
   }
   return 0;
}

int SNode::deserialize(const char* buf)
{
   char* buffer = new char[strlen(buf) + 1];
   char* tmp = buffer;

   bool stop = true;

   // file name
   strcpy(tmp, buf);
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   unsigned int namelen = atoi(tmp);

   if (stop)
   {
      delete [] buffer;
      return -1;
   }

   tmp = tmp + strlen(tmp) + 1;
   if (strlen(tmp) < namelen)
   {
      delete [] buffer;
      return -1;
   }
   tmp[namelen] = '\0';
   m_strName = tmp;

   stop = true;

   // restore dir 
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
   m_bIsDir = atoi(tmp);

   if (stop)
   {
      delete [] buffer;
      return -1;
   }
   stop = true;

   // restore timestamp
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
#ifndef WIN32
   m_llTimeStamp = atoll(tmp);
#else
   m_llTimeStamp = _atoi64(tmp);
#endif

   if (stop)
   {
      delete [] buffer;
      return -1;
   }
   stop = true;

   // restore size
   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         stop = false;
         tmp[i] = '\0';
         break;
      }
   }
#ifndef WIN32
   m_llSize = atoll(tmp);
#else
   m_llSize = _atoi64(tmp);
#endif

   // restore locations
   while (!stop)
   {
      tmp = tmp + strlen(tmp) + 1;

      stop = true;

      Address addr;
      for (unsigned int i = 0; i < strlen(tmp); ++ i)
      {
         if (tmp[i] == ',')
         {
            stop = false;
            tmp[i] = '\0';
            break;
         }
      }
      addr.m_strIP = tmp;

      if (stop)
      {
         delete [] buffer;
         return -1;
      }
      stop = true;

      tmp = tmp + strlen(tmp) + 1;
      for (unsigned int i = 0; i < strlen(tmp); ++ i)
      {
         if (tmp[i] == ',')
         {
            stop = false;
            tmp[i] = '\0';
            break;
         }
      }
      addr.m_iPort = atoi(tmp);

      m_sLocation.insert(addr);
   }

   delete [] buffer;
   return 0;
}

int SNode::serialize2(const string& file) const
{
   string tmp = file;
   size_t p = tmp.rfind('/');
   struct stat s;
   if (stat(tmp.c_str(), &s) < 0)
   {
      string cmd = string("mkdir -p ") + tmp.substr(0, p);
      system(cmd.c_str());
   }

   if (m_bIsDir)
   {
      string cmd = string("mkdir ") + file;
      system(cmd.c_str());
      return 0;
   }

   fstream ofs(file.c_str(), ios::out | ios::trunc);
   ofs << m_llSize << endl;
   ofs << m_llTimeStamp << endl;

   for (set<Address, AddrComp>::const_iterator i = m_sLocation.begin(); i != m_sLocation.end(); ++ i)
   {
      ofs << i->m_strIP << endl;
      ofs << i->m_iPort << endl;
   }
   ofs.close();

   return 0;
}

int SNode::deserialize2(const string& file)
{
   struct stat s;
   if (stat(file.c_str(), &s))
      return -1;

   size_t p = file.rfind('/');
   if (p == string::npos)
      m_strName = file;
   else
      m_strName = file.substr(p + 1);

#ifndef WIN32
   if (S_ISDIR(s.st_mode))
#else
   if ((s.st_mode & S_IFMT) == S_IFDIR)
#endif
   {
      m_bIsDir = true;
      m_llSize = s.st_size;
      m_llTimeStamp = s.st_mtime;
   }
   else
   {
      m_bIsDir = false; 

      fstream ifs(file.c_str(), ios::in);
      ifs >> m_llSize;
      ifs >> m_llTimeStamp;
      while (!ifs.eof())
      {
         Address addr;
         ifs >> addr.m_strIP;
         if (addr.m_strIP.length() == 0)
            break;
         ifs >> addr.m_iPort;
         m_sLocation.insert(addr);
      }
      ifs.close();
   }

   return 0;
}
