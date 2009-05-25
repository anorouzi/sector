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
   Yunhong Gu [gu@lac.uic.edu], last updated 04/21/2009
*****************************************************************************/


#include <algorithm>
#include <common.h>
#include <dirent.h>
#include <index.h>
#include <iostream>
#include <set>
#include <stack>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <constant.h>

using namespace std;

Index::Index()
{
   pthread_mutex_init(&m_MetaLock, NULL);
}

Index::~Index()
{
   pthread_mutex_destroy(&m_MetaLock);
}

int Index::list(const char* path, std::vector<string>& filelist)
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

         char buf[4096];
         s->second.serialize(buf);
         filelist.insert(filelist.end(), buf);
         return 1;
      }

      currdir = &(s->second.m_mDirectory);
      depth ++;
   }

   filelist.clear();
   for (map<string, SNode>::iterator i = currdir->begin(); i != currdir->end(); ++ i)
   {
      char buf[4096];
      i->second.serialize(buf);
      filelist.insert(filelist.end(), buf);
   }

   return filelist.size();
}

int Index::list_r(const char* path, std::vector<string>& filelist)
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

   if (s->second.m_bIsDir)
      return list_r(*currdir, path, filelist);

   filelist.push_back(path);
   return 1;
}

int Index::lookup(const char* path, SNode& attr)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return -3;

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
      return 1;
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

   return 1;
}

int Index::lookup(const char* path, set<Address, AddrComp>& addr)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return -3;

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
         for (set<Address>::iterator i = n->m_sLocation.begin(); i != n->m_sLocation.end(); ++ i)
            addr.insert(*i);
      }
   }

   return addr.size();
}

int Index::create(const char* path, bool isdir)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return -3;

   bool found = true;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
      {
         SNode n;
         n.m_strName = *d;
         n.m_bIsDir = true;
         n.m_llTimeStamp = 0;
         n.m_llSize = 0;
         n.m_iReadLock = n.m_iWriteLock = 0;
         (*currdir)[*d] = n;
         s = currdir->find(*d);

         found = false;
      }
      currdir = &(s->second.m_mDirectory);
   }

   // if already exist, return error
   if (found)
      return -1;

   s->second.m_bIsDir = isdir;

   return 0;
}

int Index::move(const char* oldpath, const char* newpath, const char* newname)
{
   CGuard mg(m_MetaLock);

   vector<string> olddir;
   if (parsePath(oldpath, olddir) <= 0)
      return -3;

   vector<string> newdir;
   parsePath(newpath, newdir);

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
         n.m_iReadLock = n.m_iWriteLock = 0;
         (*nd)[*d] = n;
         ns = nd->find(*d);
      }

      nd = &(ns->second.m_mDirectory);
   }

   if ((NULL == newname) || (strlen(newname) == 0))
      (*nd)[os->first] = os->second;
   else
   {
      os->second.m_strName = newname;
      (*nd)[newname] = os->second;
   }

   od->erase(os->first);

   return 1;
}

int Index::remove(const char* path, bool recursive)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return -3;

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

int Index::update(const char* fileinfo, const Address& loc, const int& type)
{
   CGuard mg(m_MetaLock);

   SNode sn;
   sn.deserialize(fileinfo);
   sn.m_sLocation.insert(loc);

   vector<string> dir;
   parsePath(sn.m_strName.c_str(), dir);

   string filename = *(dir.rbegin());
   sn.m_strName = filename;
   dir.erase(dir.begin() + dir.size() - 1);

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
      {
         if ((type == 3) || (type == 2))
         {
            // this is for new files only
            return -1;
         }

         SNode n;
         n.m_strName = *d;
         n.m_bIsDir = true;
         n.m_llTimeStamp = sn.m_llTimeStamp;
         n.m_llSize = 0;
         n.m_iReadLock = n.m_iWriteLock = 0;
         (*currdir)[*d] = n;
         s = currdir->find(*d);;
      }

      s->second.m_llTimeStamp = sn.m_llTimeStamp;

      currdir = &(s->second.m_mDirectory);
   }

   s = currdir->find(filename);
   if (s == currdir->end())
   {
      if ((type == 3) || (type == 2))
      {
         // this is for new files only
         return -1;
      }

      (*currdir)[filename] = sn;
      return 1;
   }
   else
   {
      if (type == 1)
      {
         // file exist
         return -1;
      }

      if (type == 2)
      {
         // modification to an existing copy
	 if ((s->second.m_llSize != sn.m_llSize) || (s->second.m_llTimeStamp != sn.m_llTimeStamp))
         {
            s->second.m_llSize = sn.m_llSize;
            s->second.m_llTimeStamp = sn.m_llTimeStamp;
            s->second.m_sLocation.insert(loc);
         }

         return s->second.m_sLocation.size();
      }

      if (type == 3)
      {
         // a new replica
         if (s->second.m_sLocation.find(loc) != s->second.m_sLocation.end())
            return -1;

         if ((s->second.m_llSize != sn.m_llSize) || (s->second.m_llTimeStamp != sn.m_llTimeStamp))
            return -1;

         s->second.m_sLocation.insert(loc);
         return s->second.m_sLocation.size();
      }
   }

   return -1;
}

int Index::utime(const char* path, const int64_t& ts)
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

   s->second.m_llTimeStamp = ts;
   return 1;
}

int Index::lock(const char* path, int mode)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   parsePath(path, dir);

   if (dir.empty())
      return 0;

   vector<map<string, SNode>::iterator> ptr;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      ptr.insert(ptr.end(), s);

      currdir = &(s->second.m_mDirectory);
   }

   // cannot lock a directory
   if ((*(ptr.rbegin()))->second.m_bIsDir)
      return -2;

   // if already in write lock, return error
   if ((*(ptr.rbegin()))->second.m_iWriteLock > 0)
      return -3;

   // write
   if (mode & SF_MODE::WRITE)
   {
      // if already in read lock, return error
      if ((*(ptr.rbegin()))->second.m_iReadLock > 0)
         return -4;

      for (vector<map<string, SNode>::iterator>::iterator i = ptr.begin(); i != ptr.end(); ++ i)
         (*i)->second.m_iWriteLock ++;
   }

   // read
   if (mode & SF_MODE::READ)
   {
      for (vector<map<string, SNode>::iterator>::iterator i = ptr.begin(); i != ptr.end(); ++ i)
         (*i)->second.m_iReadLock ++;
   }

   return 0;
}

int Index::unlock(const char* path, int mode)
{
   CGuard mg(m_MetaLock);

   vector<string> dir;
   parsePath(path, dir);

   if (dir.empty())
      return 0;

   vector<map<string, SNode>::iterator> ptr;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      ptr.insert(ptr.end(), s);

      currdir = &(s->second.m_mDirectory);
   }

   // cannot lock a directory
   if ((*(ptr.rbegin()))->second.m_bIsDir)
      return -2;

   // write
   if (mode & SF_MODE::WRITE)
   {
      for (vector<map<string, SNode>::iterator>::iterator i = ptr.begin(); i != ptr.end(); ++ i)
         (*i)->second.m_iWriteLock --;
   }

   // read
   if (mode & SF_MODE::READ)
   {
      for (vector<map<string, SNode>::iterator>::iterator i = ptr.begin(); i != ptr.end(); ++ i)
         (*i)->second.m_iReadLock --;
   }

   return 0;
}


int SNode::serialize(char* buf)
{
   int namelen = m_strName.length();
   sprintf(buf, "%d,%s,%d,%lld,%lld", namelen, m_strName.c_str(), m_bIsDir, m_llTimeStamp, m_llSize);
   return 0;
}

int SNode::deserialize(const char* buf)
{
   char buffer[4096];
   char* tmp = buffer;

   strcpy(tmp, buf);
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         tmp[i] = '\0';
         break;
      }
   }
   int namelen = atoi(tmp);

   tmp = tmp + strlen(tmp) + 1;
   tmp[namelen] = '\0';
   m_strName = tmp;

   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         tmp[i] = '\0';
         break;
      }
   }
   m_bIsDir = atoi(tmp);

   tmp = tmp + strlen(tmp) + 1;
   for (unsigned int i = 0; i < strlen(tmp); ++ i)
   {
      if (tmp[i] == ',')
      {
         tmp[i] = '\0';
         break;
      }
   }
   m_llTimeStamp = atoll(tmp);

   tmp = tmp + strlen(tmp) + 1;
   m_llSize = atoll(tmp);

   m_iReadLock = m_iWriteLock = 0;

   return 0;
}


int Index::serialize(ofstream& ofs, map<string, SNode>& currdir, int level)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      char* buf = new char[i->first.length() + 64];
      i->second.serialize(buf);
      ofs << level << " " << buf << endl;
      delete [] buf;

      if (i->second.m_bIsDir)
         serialize(ofs, i->second.m_mDirectory, level + 1);
   }

   return 0;
}

int Index::deserialize(ifstream& ifs, map<string, SNode>& metadata, const Address& addr)
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
      if (!sn.m_bIsDir)
         sn.m_sLocation.insert(addr);

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

int Index::scan(const string& currdir, map<std::string, SNode>& metadata)
{
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

      // skip system directory
      if (namelist[i]->d_name[0] == '.')
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

      struct stat s;
      if (stat((currdir + namelist[i]->d_name).c_str(), &s) < 0)
         continue;

      SNode sn;
      metadata[namelist[i]->d_name] = sn;
      map<std::string, SNode>::iterator mi = metadata.find(namelist[i]->d_name);
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

         ifstream ifs((currdir + namelist[i]->d_name).c_str(), ios::in);
         ifs.seekg(0, ios::end);
         mi->second.m_llSize = ifs.tellg();
      }

      free(namelist[i]);
   }
   free(namelist);

   return metadata.size();
}

int Index::merge(map<string, SNode>& currdir, map<string, SNode>& branch, string path, ofstream& left, const unsigned int& replica)
{
   for (map<string, SNode>::iterator i = branch.begin(); i != branch.end(); ++ i)
   {
      map<string, SNode>::iterator s = currdir.find(i->first);

      if (s == currdir.end())
      {
         currdir[i->first] = i->second;
      }
      else
      {
         if (i->second.m_bIsDir && s->second.m_bIsDir)
         {
            // directories with same name
            merge(s->second.m_mDirectory, i->second.m_mDirectory, path + "/" + i->first, left, replica);
         }
         else if (!(i->second.m_bIsDir) && !(s->second.m_bIsDir) 
                  && (i->second.m_llSize == s->second.m_llSize) 
                  && (i->second.m_llTimeStamp == s->second.m_llTimeStamp)
                  && (s->second.m_sLocation.size() < replica))
         {
            // files with same name, size, timestamp
            // and the number of replicas is below the threshold
            for (set<Address>::iterator a = i->second.m_sLocation.begin(); a != i->second.m_sLocation.end(); ++ a)
               s->second.m_sLocation.insert(*a);
         }
         else
         {
            // conflicts, skip this branch dir
           left << path + "/" + s->first << endl;
         }
      }
   }

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

int64_t Index::getTotalDataSize(std::map<std::string, SNode>& currdir)
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

int64_t Index::getTotalFileNum(std::map<std::string, SNode>& currdir)
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

int Index::collectDataInfo(const char* file, vector<string>& result)
{
   vector<string> dir;
   if (parsePath(file, dir) <= 0)
      return -3;

   map<string, SNode>* currdir = &m_mDirectory;
   map<string, SNode>::iterator s;
   for (vector<string>::iterator d = dir.begin();;)
   {
      s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      if (++ d != dir.end())
         currdir = &(s->second.m_mDirectory);
      else
         break;
   }

   if (!s->second.m_bIsDir)
   {
      string idx = *(dir.rbegin()) + ".idx";
      int rows = -1;
      map<string, SNode>::iterator i = currdir->find(idx);
      if (i != currdir->end())
         rows = i->second.m_llSize / 8 - 1;

      char buf[1024];
      sprintf(buf, "%s %lld %d", file, s->second.m_llSize, rows);

      for (set<Address, AddrComp>::iterator i = s->second.m_sLocation.begin(); i != s->second.m_sLocation.end(); ++ i)
         sprintf(buf + strlen(buf), " %s %d", i->m_strIP.c_str(), i->m_iPort);

      result.insert(result.end(), buf);
   }
   else
   {
      for (map<string, SNode>::iterator i = s->second.m_mDirectory.begin(); i != s->second.m_mDirectory.end(); ++ i)
      {
         int t = i->first.length();
         if ((t < 4) || (i->first.substr(t - 4, t) != ".idx"))
            collectDataInfo((file + ("/" + i->first)).c_str(), result);
      }
   }

   return result.size();
}

int Index::parsePath(const char* path, std::vector<string>& result)
{
   result.clear();

   char* token = new char[strlen(path) + 1];
   int tc = 0;
   for (unsigned int i = 0; i < strlen(path); ++ i)
   {
      // check invalid/special charactor such as * ; , etc.

      if (path[i] == '/')
      {
         if (tc > 0)
         {
            token[tc] = '\0';
            result.insert(result.end(), token);
            tc = 0;
         }
      }
      else
        token[tc ++] = path[i];
   }

   if (tc > 0)
   {
      token[tc] = '\0';
      result.insert(result.end(), token);
   }

   delete [] token;

   return result.size();
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
