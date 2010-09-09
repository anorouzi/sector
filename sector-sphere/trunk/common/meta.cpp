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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/

#include <common.h>
#include <string.h>
#include <meta.h>
#include <iostream>
using namespace std;

bool Metadata::m_pbLegalChar[256];
bool Metadata::m_bInit = Metadata::initLC();

Metadata::Metadata()
{
   CGuard::createMutex(m_MetaLock);
}

Metadata::~Metadata()
{
   CGuard::releaseMutex(m_MetaLock);
}

int Metadata::lock(const string& path, int user, int mode)
{
   CGuard mg(m_MetaLock);

   if (mode & SF_MODE::WRITE)
   {
      if (!m_mLock[path].m_sWriteLock.empty())
         return -1;

      m_mLock[path].m_sWriteLock.insert(user);
   }

   if (mode & SF_MODE::READ)
   {
      m_mLock[path].m_sReadLock.insert(user);
   }

   return 0;
}

int Metadata::unlock(const string& path, int user, int mode)
{
   CGuard mg(m_MetaLock);

   map<string, LockSet>::iterator i = m_mLock.find(path);

   if (i == m_mLock.end())
      return -1;

   if (mode & SF_MODE::WRITE)
   {
      i->second.m_sWriteLock.erase(user);
   }

   if (mode & SF_MODE::READ)
   {
      i->second.m_sReadLock.erase(user);;
   }

   if (i->second.m_sReadLock.empty() && i->second.m_sWriteLock.empty())
      m_mLock.erase(i);

   return 0;
}

int Metadata::parsePath(const string& path, vector<string>& result)
{
   result.clear();

   char* token = new char[path.length() + 1];
   int tc = 0;

   for (char* p = (char*)path.c_str(); *p != '\0'; ++ p)
   {
      if (*p == '/')
      {
         if (tc > 0)
         {
            token[tc] = '\0';
            result.insert(result.end(), token);
            tc = 0;
         }
      }
      else
      {
         // check legal characters
         if (!m_pbLegalChar[int(*p)])
         {
            delete [] token;
            return -1;
         }

         token[tc ++] = *p;
      }
   }

   if (tc > 0)
   {
      token[tc] = '\0';
      result.insert(result.end(), token);
   }

   delete [] token;

   return result.size();
}

string Metadata::revisePath(const string& path)
{
   char* newpath = new char[path.length() + 2];
   char* np = newpath;
   *np++ = '/';
   bool slash = true;

   for (char* p = (char*)path.c_str(); *p != '\0'; ++ p)
   {
      if (*p == '/')
      {
         if (!slash)
            *np++ = '/';
         slash = true;
      }
      else
      {
         // check legal characters
         if (!m_pbLegalChar[int(*p)])
         {
            delete [] newpath;
            return "";
         }

         *np++ = *p;
         slash = false;
      }
   }
   *np = '\0';

   if ((strlen(newpath) > 1) && slash)
      newpath[strlen(newpath) - 1] = '\0';

   string tmp = newpath;
   delete [] newpath;

   return tmp;
}

bool Metadata::initLC()
{
   for (int i = 0; i < 256; ++ i)
   {
      m_pbLegalChar[i] = false;
   }

   m_pbLegalChar[32] = true; // Space
   m_pbLegalChar[39] = true; // ' 
   m_pbLegalChar[40] = true; // (
   m_pbLegalChar[41] = true; // )
   m_pbLegalChar[45] = true; // -
   m_pbLegalChar[46] = true; // .

   // 0 - 9
   for (int i = 48; i <= 57; ++ i)
   {
      m_pbLegalChar[i] = true;
   }

   m_pbLegalChar[64] = true; // @

   // A - Z
   for (int i = 65; i <= 90; ++ i)
   {
      m_pbLegalChar[i] = true;
   }

   m_pbLegalChar[95] = true; // _

   // a - z
   for (int i = 97; i <= 122; ++ i)
   {
      m_pbLegalChar[i] = true;
   }

   m_pbLegalChar[126] = true; // ~

   return true;
}
