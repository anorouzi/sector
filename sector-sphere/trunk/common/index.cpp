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
   Yunhong Gu, last updated 09/02/2010
*****************************************************************************/


#include <algorithm>
#include <common.h>
#include <index.h>
#include <set>
#include <stack>
#include <sys/types.h>
#include <sys/stat.h>
#include <cstring>
#include <time.h>
#include <sector.h>

#ifndef WIN32
   #include <dirent.h>
   #include <unistd.h>
#else
   #include <windows.h>
   #include <tchar.h> 
   #include <stdio.h>
   #include <strsafe.h>
#endif

using namespace std;

Index::Index()
{
}

Index::~Index()
{
}

int Index::list(const string& path, vector<string>& filelist)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   unsigned int depth = 1;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      map<string, SNode>::iterator s = currdir->find(*d);
      if (s == currdir->end())
         return SectorError::E_NOEXIST;

      if (!s->second.m_bIsDir)
      {
         if (depth != dir.size())
            return SectorError::E_NOEXIST;

         char* buf = NULL;
         if (s->second.serialize(buf) >= 0)
            filelist.insert(filelist.end(), buf);
         delete [] buf;
         return 1;
      }

      currdir = &(s->second.m_mDirectory);
      depth ++;
   }

   filelist.clear();
   for (map<string, SNode>::iterator i = currdir->begin(); i != currdir->end(); ++ i)
   {
      char* buf = NULL;
      if (i->second.serialize(buf) >= 0)
         filelist.insert(filelist.end(), buf);
      delete [] buf;
   }

   return filelist.size();
}

int Index::list_r(const string& path, vector<string>& filelist)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return SectorError::E_NOEXIST;

      currdir = &(s->second.m_mDirectory);
   }

   if (dir.empty() || s->second.m_bIsDir)
      return list_r(*currdir, path, filelist);

   filelist.push_back(path);
   return 1;
}

int Index::lookup(const string& path, SNode& attr)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   if (dir.empty())
   {
      // stat on the root directory "/"
      attr.m_strName = "/";
      attr.m_bIsDir = true;
      attr.m_llSize = 0;
      attr.m_llTimeStamp = 0;
      for (map<string, SNode>::iterator i = m_mDirectory.begin(); i != m_mDirectory.end(); ++ i)
      {
         attr.m_llSize += i->second.m_llSize;
         if (attr.m_llTimeStamp < i->second.m_llTimeStamp)
            attr.m_llTimeStamp = i->second.m_llTimeStamp;
      }
      return m_mDirectory.size();
   }

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   attr.m_strName = s->second.m_strName;
   attr.m_bIsDir = s->second.m_bIsDir;
   attr.m_llSize = s->second.m_llSize;
   attr.m_llTimeStamp = s->second.m_llTimeStamp;
   attr.m_sLocation = s->second.m_sLocation;

   return s->second.m_mDirectory.size();
}

int Index::lookup(const string& path, set<Address, AddrComp>& addr)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   stack<SNode*> scanmap;
   scanmap.push(&(s->second));

   while (!scanmap.empty())
   {
      SNode* n = scanmap.top();
      scanmap.pop();

      if (n->m_bIsDir)
      {
         for (map<string, SNode>::iterator i = n->m_mDirectory.begin(); i != n->m_mDirectory.end(); ++ i)
            scanmap.push(&(i->second));
      }
      else
      {
         for (set<Address, AddrComp>::iterator i = n->m_sLocation.begin(); i != n->m_sLocation.end(); ++ i)
            addr.insert(*i);
      }
   }

   return addr.size();
}

int Index::create(const SNode& node)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(node.m_strName.c_str(), dir) <= 0)
      return -3;

   if (dir.empty())
      return -1;

   bool found = true;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   string filename;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
      {
         SNode n;
         n.m_strName = *d;
         n.m_bIsDir = true;
         n.m_llTimeStamp = time(NULL);
         n.m_llSize = 0;
         (*currdir)[*d] = n;
         s = currdir->find(*d);

         filename = *d;

         found = false;
      }
      currdir = &(s->second.m_mDirectory);
   }

   // if already exist, return error
   if (found)
      return -1;

   // node initially contains full path name, revise it to file name only
   s->second = node;
   s->second.m_strName = filename;

   return 0;
}

int Index::move(const string& oldpath, const string& newpath, const string& newname)
{
   CGuard mg(m_MetaLock);

   vector<string> olddir;
   if (parsePath(oldpath, olddir) <= 0)
      return -3;

   if (olddir.empty())
      return -1;

   vector<string> newdir;
   if (parsePath(newpath, newdir) < 0)
      return -3;

   map<string, SNode>* od = &m_mDirectory;
   map<string, SNode>::iterator os;
   for (vector<string>::iterator d = olddir.begin();;)
   {
      os = od->find(*d);
      if (os == od->end())
         return -1;

      if (++ d == olddir.end())
         break;

      od = &(os->second.m_mDirectory);
   }

   map<string, SNode>* nd = &m_mDirectory;
   map<string, SNode>::iterator ns;
   for (vector<string>::iterator d = newdir.begin(); d != newdir.end(); ++ d)
   {
      ns = nd->find(*d);
      if (ns == nd->end())
      {
         SNode n;
         n.m_strName = *d;
         n.m_bIsDir = true;
         n.m_llTimeStamp = 0;
         n.m_llSize = 0;
         (*nd)[*d] = n;
         ns = nd->find(*d);
      }

      nd = &(ns->second.m_mDirectory);
   }

   if (newname.length() == 0)
      (*nd)[os->first] = os->second;
   else
   {
      os->second.m_strName = newname;
      (*nd)[newname] = os->second;
   }

   od->erase(os->first);

   return 1;
}

int Index::remove(const string& path, bool recursive)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return SectorError::E_INVALID;

   if (dir.empty())
      return -1;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); ; )
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      if (++ d == dir.end())
         break;

      currdir = &(s->second.m_mDirectory);
   }

   if (s->second.m_bIsDir)
   {
      if (recursive)
         currdir->erase(s);
      else
         return -1;
   }
   else
   {
      currdir->erase(s);
   }

   return 0;
}

int Index::addReplica(const string& path, const int64_t& ts, const int64_t& size, const Address& addr)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   parsePath(path.c_str(), dir);
   if (dir.empty())
      return -1;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
      {
         // file does not exist, return error
         return SectorError::E_NOEXIST;
      }
      currdir = &(s->second.m_mDirectory);
   }

   if ((s->second.m_llSize != size) || (s->second.m_llTimeStamp != ts))
      return -1;

   s->second.m_sLocation.insert(addr);

   return 0;
}

int Index::removeReplica(const string& path, const Address& addr)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   parsePath(path.c_str(), dir);
   if (dir.empty())
      return -1;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
      {
         // file does not exist, return error
         return SectorError::E_NOEXIST;
      }
      currdir = &(s->second.m_mDirectory);
   }

   s->second.m_sLocation.erase(addr);

   return 0;
}

int Index::update(const string& path, const int64_t& ts, const int64_t& size)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   parsePath(path, dir);

   if (dir.empty())
      return 0;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   // sometime it may be necessary to update timestamp only. In this case size should be set to <0.
   if (size >= 0)
      s->second.m_llSize = size;

   s->second.m_llTimeStamp = ts;

   return 1;
}

int Index::serialize(const string& path, const string& dstfile)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   ofstream ofs(dstfile.c_str());
   if (ofs.bad() || ofs.fail())
      return -1;

   serialize(ofs, *currdir, 1);

   ofs.close();
   return 0;
}

int Index::deserialize(const string& path, const string& srcfile,  const Address* addr)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   ifstream ifs(srcfile.c_str());
   if (ifs.bad() || ifs.fail())
      return -1;

   deserialize(ifs, *currdir, addr);

   ifs.close();
   return 0;
}

int Index::scan(const string& datadir, const string& metadir)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(metadir, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   scan(datadir, *currdir);

   return 0;
}

int Index::merge(const string& path, Metadata* meta, const unsigned int& replica)
{
   CGuard mg(m_MetaLock);

   merge(m_mDirectory, ((Index*)meta)->m_mDirectory, replica);

   return 0;
}

int Index::substract(const string& path, const Address& addr)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   substract(*currdir, addr);

   return 0;
}

int64_t Index::getTotalDataSize(const string& path)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   return getTotalDataSize(*currdir);
}

int64_t Index::getTotalFileNum(const string& path)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   return getTotalFileNum(*currdir);
}

int Index::collectDataInfo(const string& file, vector<string>& result)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(file, dir) <= 0)
      return -3;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>* updir = NULL;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      updir = currdir;
      currdir = &(s->second.m_mDirectory);
   }

   if (!s->second.m_bIsDir)
   {
      string idx = *dir.rbegin() + ".idx";
      int rows = -1;
      map<string, SNode>::iterator i = updir->find(idx);
      if (i != updir->end())
         rows = i->second.m_llSize / 8 - 1;

      char buf[1024];
      sprintf(buf, "%s %lld %d", file.c_str(), (long long int)s->second.m_llSize, rows);

      for (set<Address, AddrComp>::iterator j = s->second.m_sLocation.begin(); j != s->second.m_sLocation.end(); ++ j)
         sprintf(buf + strlen(buf), " %s %d", j->m_strIP.c_str(), j->m_iPort);

      result.push_back(buf);
   }

   return collectDataInfo(file, *currdir, result);
}

int Index::checkReplica(const string& path, vector<string>& under, vector<string>& over)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return SectorError::E_INVALID;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      currdir = &(s->second.m_mDirectory);
   }

   return checkReplica(path, *currdir, under, over);
}

int Index::getSlaveMeta(Metadata* branch, const Address& addr)
{
   CGuard mg(m_MetaLock);

   vector<string> path;
   return getSlaveMeta(m_mDirectory, path, ((Index*)branch)->m_mDirectory, addr);
}

///////////////////////////////////////////////////////////////////////////////////////

int Index::serialize(ofstream& ofs, map<string, SNode>& currdir, int level)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      char* buf = NULL;
      if (i->second.serialize(buf) >= 0)
         ofs << level << " " << buf << endl;
      delete [] buf;

      if (i->second.m_bIsDir)
         serialize(ofs, i->second.m_mDirectory, level + 1);
   }

   return 0;
}

int Index::deserialize(ifstream& ifs, map<string, SNode>& metadata, const Address* addr)
{
   vector<string> dirs;
   dirs.resize(1024);
   map<string, SNode>* currdir = &metadata;
   int currlevel = 1;

   while (!ifs.eof())
   {
      char tmp[4096];
      tmp[4095] = 0;
      char* buf = tmp;

      ifs.getline(buf, 4096);
      int len = strlen(buf);
      if ((len <= 0) || (len >= 4095))
         continue;

      for (int i = 0; i < len; ++ i)
      {
         if (buf[i] == ' ')
         {
            buf[i] = '\0';
            break;
         }
      }

      int level = atoi(buf);

      SNode sn;
      sn.deserialize(buf + strlen(buf) + 1);
      if ((!sn.m_bIsDir) && (NULL != addr))
      {
         sn.m_sLocation.clear();
         sn.m_sLocation.insert(*addr);
      }

      if (level == currlevel)
      {
         (*currdir)[sn.m_strName] = sn;
         dirs[level] = sn.m_strName;
      }
      else if (level == currlevel + 1)
      {
         map<string, SNode>::iterator s = currdir->find(dirs[currlevel]);
         currdir = &(s->second.m_mDirectory);
         currlevel = level;

         (*currdir)[sn.m_strName] = sn;
         dirs[level] = sn.m_strName;
      }
      else if (level < currlevel)
      {
         currdir = &metadata;

         for (int i = 1; i < level; ++ i)
         {
            map<string, SNode>::iterator s = currdir->find(dirs[i]);
            currdir = &(s->second.m_mDirectory);
         }
         currlevel = level;

         (*currdir)[sn.m_strName] = sn;
         dirs[level] = sn.m_strName;
      }
   }

   return 0;
}

int Index::scan(const string& currdir, map<string, SNode>& metadata)
{
#ifndef WIN32
   dirent **namelist;
   int n = scandir(currdir.c_str(), &namelist, 0, alphasort);

   if (n < 0)
      return -1;

   metadata.clear();

   for (int i = 0; i < n; ++ i)
   {
      // skip "." and ".."
      if ((strcmp(namelist[i]->d_name, ".") == 0) || (strcmp(namelist[i]->d_name, "..") == 0))
      {
         free(namelist[i]);
         continue;
      }

      // check file name
      bool bad = false;
      for (char *p = namelist[i]->d_name, *q = namelist[i]->d_name + strlen(namelist[i]->d_name); p != q; ++ p)
      {
         if ((*p == 10) || (*p == 13))
         {
            bad = true;
            break;
         }
      }
      if (bad)
         continue;

      struct stat64 s;
      if (stat64((currdir + namelist[i]->d_name).c_str(), &s) < 0)
         continue;

      // skip system file and directory
      if (S_ISDIR(s.st_mode) && (namelist[i]->d_name[0] == '.'))
      {
         free(namelist[i]);
         continue;
      }

      SNode sn;
      metadata[namelist[i]->d_name] = sn;
      map<string, SNode>::iterator mi = metadata.find(namelist[i]->d_name);
      mi->second.m_strName = namelist[i]->d_name;

      mi->second.m_llSize = s.st_size;
      mi->second.m_llTimeStamp = s.st_mtime;

      if (S_ISDIR(s.st_mode))
      {
         mi->second.m_bIsDir = true;
         scan(currdir + namelist[i]->d_name + "/", mi->second.m_mDirectory);
      }
      else
      {
         mi->second.m_bIsDir = false;
      }

      free(namelist[i]);
   }
   free(namelist);

   return metadata.size();
#else
   WIN32_FIND_DATA ffd;
   LARGE_INTEGER filesize;
   TCHAR szDir[MAX_PATH];
   size_t length_of_arg;
   HANDLE hFind = INVALID_HANDLE_VALUE;
   DWORD dwError=0;

   StringCchLength(currdir.c_str(), MAX_PATH, &length_of_arg);

   if (length_of_arg > (MAX_PATH - 3))
      return -1;

   StringCchCopy(szDir, MAX_PATH, currdir.c_str());
   StringCchCat(szDir, MAX_PATH, TEXT("\\*"));

   // Find the first file in the directory.
   hFind = FindFirstFile(szDir, &ffd);
   if (INVALID_HANDLE_VALUE == hFind) 
      return dwError;
   
   // List all the files in the directory with some info about them.
   do
   {
      SNode sn;
      metadata[ffd.cFileName] = sn;
      map<string, SNode>::iterator mi = metadata.find(ffd.cFileName);
      mi->second.m_strName = ffd.cFileName;
      mi->second.m_llSize = 0;
      mi->second.m_llTimeStamp = 0;

      if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
      {
         mi->second.m_bIsDir = true;
         scan(ffd.cFileName, mi->second.m_mDirectory);
      }
      else
      {
         filesize.LowPart = ffd.nFileSizeLow;
         filesize.HighPart = ffd.nFileSizeHigh;
     	 mi->second.m_llSize = filesize.QuadPart;

         LARGE_INTEGER ts;
         ts.LowPart = ffd.ftLastWriteTime.dwLowDateTime;
         ts.HighPart = ffd.ftLastWriteTime.dwHighDateTime;
         mi->second.m_llTimeStamp = ts.QuadPart;
      }
   }
   while (FindNextFile(hFind, &ffd) != 0);
 
   dwError = GetLastError();
   if (dwError != ERROR_NO_MORE_FILES) 
      return -1;

   FindClose(hFind);
   return metadata.size();
#endif
}

int Index::merge(map<string, SNode>& currdir, map<string, SNode>& branch, const unsigned int& replica)
{
   vector<string> tbd;

   for (map<string, SNode>::iterator i = branch.begin(); i != branch.end(); ++ i)
   {
      map<string, SNode>::iterator s = currdir.find(i->first);

      if (s == currdir.end())
      {
         currdir[i->first] = i->second;
         tbd.push_back(i->first);
      }
      else
      {
         if (i->second.m_bIsDir && s->second.m_bIsDir)
         {
            // directories with same name
            merge(s->second.m_mDirectory, i->second.m_mDirectory, replica);
         }
         else if (!(i->second.m_bIsDir) && !(s->second.m_bIsDir) 
                  && (i->second.m_llSize == s->second.m_llSize) 
                  && (i->second.m_llTimeStamp == s->second.m_llTimeStamp))
                  //&& (s->second.m_sLocation.size() < replica))
         {
            // files with same name, size, timestamp
            // and the number of replicas is below the threshold
            for (set<Address, AddrComp>::iterator a = i->second.m_sLocation.begin(); a != i->second.m_sLocation.end(); ++ a)
               s->second.m_sLocation.insert(*a);
            tbd.push_back(i->first);
         }
      }
   }

   for (vector<string>::iterator i = tbd.begin(); i != tbd.end(); ++ i)
      branch.erase(*i);

   return 0;
}

int Index::substract(map<string, SNode>& currdir, const Address& addr)
{
   vector<string> tbd;

   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
      {
         i->second.m_sLocation.erase(addr);
         if (i->second.m_sLocation.empty())
            tbd.insert(tbd.end(), i->first);
      }
      else
         substract(i->second.m_mDirectory, addr);
   }

   for (vector<string>::iterator i = tbd.begin(); i != tbd.end(); ++ i)
      currdir.erase(*i);

   return 0;
}

int64_t Index::getTotalDataSize(map<string, SNode>& currdir)
{
   int64_t size = 0;

   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
         size += i->second.m_llSize;
      else
         size += getTotalDataSize(i->second.m_mDirectory);
   }

   return size;
}

int64_t Index::getTotalFileNum(map<string, SNode>& currdir)
{
   int64_t num = 0;

   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
         num ++;
      else
         num += getTotalFileNum(i->second.m_mDirectory);
   }

   return num;
}

int Index::collectDataInfo(const string& path, map<string, SNode>& currdir, vector<string>& result)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
      {
         // skip system files
         if (i->first.c_str()[0] == '.')
           continue;

         // ignore index file
         int t = i->first.length();
         if ((t > 4) && (i->first.substr(t - 4, t) == ".idx"))
            continue;

         string idx = i->first + ".idx";
         int rows = -1;
         map<string, SNode>::iterator j = currdir.find(idx);
         if (j != currdir.end())
            rows = j->second.m_llSize / 8 - 1;

         char buf[1024];
         sprintf(buf, "%s %lld %d", (path + "/" + i->first).c_str(), (long long int)i->second.m_llSize, rows);

         for (set<Address, AddrComp>::iterator k = i->second.m_sLocation.begin(); k != i->second.m_sLocation.end(); ++ k)
            sprintf(buf + strlen(buf), " %s %d", k->m_strIP.c_str(), k->m_iPort);

         result.push_back(buf);
      }
      else
         collectDataInfo((path + "/" + i->first).c_str(), i->second.m_mDirectory, result);
   }

   return result.size();
}

int Index::checkReplica(const string& path, map<string, SNode>& currdir, vector<string>& under, vector<string>& over)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      string abs_path = path;
      if (path == "/")
         abs_path += i->first;
      else
         abs_path += "/" + i->first;

      if ((!i->second.m_bIsDir) || (i->second.m_mDirectory.find(".nosplit") != i->second.m_mDirectory.end()))
      {
         // replicate a file according to the number of specified replicas
         // or if this is a directory and it contains a file called ".nosplit", the whole directory will be replicated together

         unsigned int curr_rep_num = 0;
         unsigned int target_rep_num = 0;
         if (i->second.m_mDirectory.find(".nosplit") != i->second.m_mDirectory.end())
         {
            curr_rep_num = i->second.m_mDirectory[".nosplit"].m_sLocation.size();
            target_rep_num = i->second.m_mDirectory[".nosplit"].m_iReplicaNum;
         }
         else
         {
            curr_rep_num = i->second.m_sLocation.size();
            target_rep_num = i->second.m_iReplicaNum;
         }

         if (curr_rep_num < target_rep_num)
            under.push_back(abs_path);
         else if (curr_rep_num > target_rep_num)
            over.push_back(abs_path);
      }
      else
         checkReplica(abs_path, i->second.m_mDirectory, under, over);
   }

   return 0;
}

int Index::list_r(map<string, SNode>& currdir, const string& path, vector<string>& filelist)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
         filelist.insert(filelist.end(), path + "/" + i->second.m_strName);
      else
         list_r(i->second.m_mDirectory, path + "/" + i->second.m_strName, filelist);
   }

   return filelist.size();
}

int Index::getSlaveMeta(map<string, SNode>& currdir, vector<string>& path, map<string, SNode>& target, const Address& addr)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
      {
         if (i->second.m_sLocation.find(addr) != i->second.m_sLocation.end());
         {
            map<string, SNode>* currdir = &target;
            for (vector<string>::iterator d = path.begin(); d != path.end(); ++ d)
            {
               map<string, SNode>::iterator s = currdir->find(*d);
               if (s == currdir->end())
               {
                  SNode n;
                  n.m_strName = *d;
                  n.m_bIsDir = true;
                  n.m_llTimeStamp = time(NULL);
                  n.m_llSize = 0;
                  (*currdir)[*d] = n;
                  s = currdir->find(*d);
               }

               currdir = &(s->second.m_mDirectory);
            }

            (*currdir)[i->first] = i->second;
         }
      }
      else
      {
         path.push_back(i->first);
         getSlaveMeta(i->second.m_mDirectory, path, target, addr);
         path.erase(path.begin() + path.size() - 1);
      }
   }

   return 0;
}

void Index::refreshRepSetting(const string& path, int default_num, int default_dist, map<string, int>& rep_num, map<string, int>& rep_dist)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return;

      currdir = &(s->second.m_mDirectory);
   }

   refreshRepSetting(path, *currdir, default_num, default_dist, rep_num, rep_dist);
}

int Index::refreshRepSetting(const string& path, map<string, SNode>& currdir, int default_num, int default_dist, map<string, int>& rep_num, map<string, int>& rep_dist)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      string abs_path = path;
      if (path == "/")
         abs_path += i->first;
      else
         abs_path += "/" + i->first;

      i->second.m_iReplicaNum = default_num;

      for (map<string, int>::const_iterator rn = rep_num.begin(); rn != rep_num.end(); ++ rn)
      {
         if (WildCard::contain(rn->first, abs_path))
         {
            i->second.m_iReplicaNum = rn->second;
            break;
         }
      }

      i->second.m_iReplicaDist = default_dist;

      for (map<string, int>::const_iterator rd = rep_dist.begin(); rd != rep_dist.end(); ++ rd)
      {
         if (WildCard::contain(rd->first, abs_path))
         {
            i->second.m_iReplicaDist = rd->second;
            break;
         }
      }

      if (i->second.m_bIsDir)
         refreshRepSetting(abs_path, i->second.m_mDirectory, default_num, default_dist, rep_num, rep_dist);
   }

   return 0;
}
