/*****************************************************************************
Copyright � 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/04/2008
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


using namespace std;

int Index::list(const char* path, std::vector<string>& filelist)
{
   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return -3;

   map<string, SNode>* currdir = &m_mDirectory;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      map<string, SNode>::iterator s = currdir->find(*d);
      if (s == currdir->end())
         return -1;

      if (!s->second.m_bIsDir)
         return -2;

      currdir = &(s->second.m_mDirectory);
   }

   filelist.clear();
   for (map<string, SNode>::iterator i = currdir->begin(); i != currdir->end(); ++ i)
   {
      char buf[128];
      i->second.serialize(buf);
      filelist.insert(filelist.end(), buf);
   }

   return filelist.size();
}

int Index::lookup(const char* path, SNode& attr)
{
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

   attr.m_strName = s->second.m_strName;
   attr.m_bIsDir = s->second.m_bIsDir;
   attr.m_llSize = s->second.m_llSize;
   attr.m_llTimeStamp = s->second.m_llTimeStamp;
   attr.m_sLocation = s->second.m_sLocation;

   return 1;
}

int Index::lookup(const char* path, set<Address, AddrComp>& addr)
{
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
         n.m_bIsDir = false;
         (*currdir)[*d] = n;
         s = currdir->find(*d);;

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

int Index::move(const char* oldpath, const char* newpath)
{
   vector<string> olddir;
   if (parsePath(oldpath, olddir) <= 0)
      return -3;

   vector<string> newdir;
   if (parsePath(newpath, newdir) <= 0)
      return -3;

   map<string, SNode>* od = &m_mDirectory;
   map<string, SNode>::iterator os;
   for (vector<string>::iterator d = olddir.begin(); d != olddir.end(); ++ d)
   {
      os = od->find(*d);
      if (os == od->end())
         return -1;

      od = &(os->second.m_mDirectory);
   }

   map<string, SNode>* nd = &m_mDirectory;
   map<string, SNode>::iterator ns;
   for (vector<string>::iterator d = newdir.begin(); d != newdir.end(); ++ d)
   {
      ns = nd->find(*d);
      if (ns == nd->end())
         break;

      nd = &(ns->second.m_mDirectory);
   }

   return 1;
}

int Index::remove(const char* path, bool recursive)
{
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

int Index::addCopy(const char* path, const Address& loc)
{
   return 0;
}
int Index::eraseCopy(const char* path, const Address& loc)
{
   return 0;
}

int Index::update(const char* fileinfo, const Address& loc)
{
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
         SNode n;
         n.m_strName = *d;
         n.m_bIsDir = false;
         (*currdir)[*d] = n;
         s = currdir->find(*d);;
      }
      currdir = &(s->second.m_mDirectory);
   }

   s = currdir->find(filename);
   if (s == currdir->end())
   {
      (*currdir)[filename] = sn;
      return 1;
   }
   else
   {
      // check size/timestamp, reject name conflicts
      if (s->second.m_llSize != sn.m_llSize)
         return -1;

      //s->second.m_llSize = sn.m_llSize;
      //s->second.m_llTimeStamp = sn.m_llTimeStamp;
      s->second.m_sLocation.insert(loc);
      return s->second.m_sLocation.size();
   }

   return 0;
}

int Index::lock(const char* path, int mode)
{
   vector<string> dir;
   parsePath(path, dir);

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
   if (2 == mode)
   {
      // if already in read lock, return error
      if ((*(ptr.rbegin()))->second.m_iReadLock > 0)
         return -4;

      for (vector<map<string, SNode>::iterator>::iterator i = ptr.begin(); i != ptr.end(); ++ i)
         (*i)->second.m_iWriteLock ++;
   }

   // read
   if (1 == mode)
   {
      for (vector<map<string, SNode>::iterator>::iterator i = ptr.begin(); i != ptr.end(); ++ i)
         (*i)->second.m_iReadLock ++;
   }

   return 0;
}

int Index::unlock(const char* path, int mode)
{
   vector<string> dir;
   parsePath(path, dir);

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
   if (2 == mode)
   {
      for (vector<map<string, SNode>::iterator>::iterator i = ptr.begin(); i != ptr.end(); ++ i)
         (*i)->second.m_iWriteLock --;
   }

   // read
   if (1 == mode)
   {
      for (vector<map<string, SNode>::iterator>::iterator i = ptr.begin(); i != ptr.end(); ++ i)
         (*i)->second.m_iReadLock --;
   }

   return 0;
}

int SNode::serialize(char* buf)
{
   sprintf(buf, "%s,%d,%lld,%lld", m_strName.c_str(), m_bIsDir, m_llTimeStamp, m_llSize);
   return 0;
}

int SNode::deserialize(const char* buf)
{
   char buffer[128];
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

   return 0;
}

int Index::serialize(ofstream& ofs, map<string, SNode>& currdir, int level)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      char* buf = new char[128];
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
      char tmp[128];
      char* buf = tmp;

      ifs.getline(buf, 128);

      for (int i = 0; i < 128; ++ i)
      {
         if (buf[i] == ' ')
            buf[i] = '\0';
      }

      int level = atoi(buf);

      SNode sn;
      sn.deserialize(buf + strlen(buf) + 1);

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

   // assign address to the file locations
   stack<SNode*> scanmap;

   for (map<string, SNode>::iterator i = metadata.begin(); i != metadata.end(); ++ i)
      scanmap.push(&(i->second));

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
         n->m_sLocation.insert(addr);
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

      struct stat s;
      stat((currdir + namelist[i]->d_name).c_str(), &s);

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

         ifstream ifs((currdir + namelist[i]->d_name).c_str());
         ifs.seekg(0, ios::end);
         mi->second.m_llSize = ifs.tellg();
      }

      free(namelist[i]);
   }
   free(namelist);

   return metadata.size();
}

int Index::merge(map<string, SNode>& currdir, map<string, SNode>& branch)
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
            merge(s->second.m_mDirectory, i->second.m_mDirectory);
         }
         else if (!(i->second.m_bIsDir) && !(s->second.m_bIsDir) && (i->second.m_llSize != s->second.m_llSize))
         {
            // files with same name, size, timestamp, ...
            for (set<Address>::iterator a = i->second.m_sLocation.begin(); a != i->second.m_sLocation.end(); ++ a)
               s->second.m_sLocation.insert(*a);
         }
         else
         {
            // conflicts, skip this branch dir
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

int Index::parsePath(const char* path, std::vector<string>& result)
{
   result.clear();

   char token[64];
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

   return result.size();
}