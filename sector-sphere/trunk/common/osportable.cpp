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
   Yunhong Gu, last updated 10/04/2010
*****************************************************************************/

#include <osportable.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifndef WIN32
   #include <dirent.h>
   #include <unistd.h>
#else
   #include <windows.h>
   #include <tchar.h>
   #include <strsafe.h>
   #include <direct.h>
#endif

using namespace std;

#ifdef WIN32
#if _WIN32_WINNT <= _WIN32_WINNT_WS03

const char *inet_ntop(int af, const void *src, char *dst, socklen_t cnt)
{
   if (af == AF_INET)
   {
      struct sockaddr_in in;
      memset(&in, 0, sizeof(in));
      n.sin_family = AF_INET;
      memcpy(&in.sin_addr, src, sizeof(struct in_addr));
      getnameinfo((struct sockaddr *)&in, sizeof(struct sockaddr_in), dst, cnt, NULL, 0, NI_NUMERICHOST);
      return dst;
   }
   else if (af == AF_INET6)
   {
      struct sockaddr_in6 in;
      memset(&in, 0, sizeof(in));
      in.sin6_family = AF_INET6;
      memcpy(&in.sin6_addr, src, sizeof(struct in_addr6));
      getnameinfo((struct sockaddr *)&in, sizeof(struct sockaddr_in6), dst, cnt, NULL, 0, NI_NUMERICHOST);
      return dst;
   }

   return NULL;
}

int inet_pton(int af, const char* s, void* d)
{
   if (af == AF_INET)
   {
      ((sockaddr_in*)d)->s_addr = inet_addr(s);
      return 0;
   }
   else if (af == AF_INET6)
   {

   }

   return -1;
}

#endif
#endif


int LocalFS::mkdir(const std::string& path)
{
#ifndef WIN32
   if ((-1 == ::mkdir(path.c_str(), S_IRWXU)) && (errno != EEXIST))
      return -1;
   return 0;
#else
   if ((-1 == ::_mkdir(path.c_str())) && (errno != EEXIST))
      return -1;
   return 0;
#endif
}

int LocalFS::rmdir(const std::string& path)
{
   string cmd;

#ifndef WIN32
   cmd = "rm -rf " + reviseSysCmdPath(path);
#else
   cmd = string("rmdir /Q /S \"") + reviseSysCmdPath(path);
#endif

   system(cmd.c_str());

   return 0;
}

int LocalFS::clean_dir(const std::string& path)
{
   string cmd;

#ifndef WIN32
   cmd = "rm -rf " + reviseSysCmdPath(path) + "/*";
#else
   cmd = string("rmdir /Q /S \"") + reviseSysCmdPath(path) + "\*";
#endif

   system(cmd.c_str());

   return 0;
}

int LocalFS::list_dir(const std::string& path, vector<SNode>& filelist)
{
#ifndef WIN32
   dirent **namelist;
   int n = scandir(path.c_str(), &namelist, 0, alphasort);

   if (n < 0)
      return -1;

   filelist.clear();

   for (int i = 0; i < n; ++ i)
   {
      // skip "." and ".."
      if ((strcmp(namelist[i]->d_name, ".") == 0) || (strcmp(namelist[i]->d_name, "..") == 0))
      {
         free(namelist[i]);
         continue;
      }

      SNode s;
      if (stat(path + "/" + namelist[i]->d_name, s) >= 0)
         filelist.push_back(s);

      free(namelist[i]);
   }
   free(namelist);

   return 0;
#else
   WIN32_FIND_DATA ffd;
   TCHAR szDir[MAX_PATH];
   size_t length_of_arg;
   HANDLE hFind = INVALID_HANDLE_VALUE;
   DWORD dwError=0;

   StringCchLength(path.c_str(), MAX_PATH, &length_of_arg);

   if (length_of_arg > (MAX_PATH - 3))
      return -1;

   StringCchCopy(szDir, MAX_PATH, path.c_str());
   StringCchCat(szDir, MAX_PATH, TEXT("\\*"));

   // Find the first file in the directory.
   hFind = FindFirstFile(szDir, &ffd);
   if (INVALID_HANDLE_VALUE == hFind)
      return dwError;

   // List all the files in the directory with some info about them.
   do
   {
      SNode s;
      if (stat(path + "/" + ffd.cFileName, s) >= 0)
         filelist.push_back(s);
   }
   while (FindNextFile(hFind, &ffd) != 0);

   return 0;
#endif
}

int LocalFS::stat(const std::string& path, SNode& sn)
{
#ifndef WIN32
   struct stat64 st;
   if (-1 == stat64(path.c_str(), &st))
      return -1;
#else
   struct _stat64 st;
   if (-1 == _stat64(path.c_str(), &st))
      return -1;
#endif

   size_t pos = path.rfind('/');
   if (pos == string::npos)
      sn.m_strName = path;
   else
      sn.m_strName = path.substr(pos + 1, path.length() - pos);

   sn.m_bIsDir = S_ISDIR(st.st_mode) ? 1 : 0;
   sn.m_llTimeStamp = st.st_mtime;
   sn.m_llSize = st.st_size;

   Address addr;
   addr.m_strIP = "127.0.0.1";
   addr.m_iPort = 0;
   sn.m_sLocation.insert(addr);

   return 0;
}

string LocalFS::reviseSysCmdPath(const string& path)
{
   string rpath;

#ifndef WIN32
   for (const char *p = path.c_str(), *q = path.c_str() + path.length(); p != q; ++ p)
   {
      if ((*p == 32) || (*p == 34) || (*p == 38) || (*p == 39))
         rpath.append(1, char(92));
      rpath.append(1, *p);
   }
#else
   rpath = path;
#endif

   return rpath;
}

