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


#include <algorithm>
#include <common.h>
#include <dirent.h>
#ifndef WIN32
    #include <unistd.h>
#endif
#include <index2.h>
#include <set>
#include <sys/types.h>
#include <sys/stat.h>
#include <cstring>
#ifdef WIN32
    #include "statfs.h"
#endif

using namespace std;

Index2::Index2():
m_strMetaPath("/tmp")
{
}

Index2::~Index2()
{
}

void Index2::init(const string& path)
{
   m_strMetaPath = path;
}

void Index2::clear()
{
#ifndef WIN32
   string cmd ("rm -rf " + m_strMetaPath);
#else
   string winpath(m_strMetaPath);
   unix_to_win_path(winpath);
   string cmd("del /F /Q \"" + winpath + "\"");
#endif
   system(cmd.c_str());
}

int Index2::list(const string& path, vector<string>& filelist)
{
   string item = m_strMetaPath + "/" + path;

   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) < 0)
      return SectorError::E_NOEXIST;

   if (!S_ISDIR(s.st_mode))
   {
      SNode sn;
      sn.deserialize2(item);
      char* buf = NULL;
      sn.serialize(buf);
      filelist.insert(filelist.end(), buf);
      delete [] buf;
      return 1;
   }

   dirent **namelist;
   int n = scandir(item.c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   filelist.clear();

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      SNode sn;
      if (sn.deserialize2(item + "/" + namelist[i]->d_name) < 0)
      {
         free(namelist[i]);
         continue;
      }

      char* buf = NULL;
      sn.serialize(buf);
      filelist.insert(filelist.end(), buf);
      delete [] buf;

      free(namelist[i]);
   }
   free(namelist);

   return filelist.size();
}

int Index2::list_r(const string& path, vector<string>& filelist)
{
   string item = m_strMetaPath + "/" + path;

   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) < 0)
      return SectorError::E_NOEXIST;

   if (!S_ISDIR(s.st_mode))
   {
      filelist.push_back(path);
      return 1;
   }

   dirent **namelist;
   int n = scandir(item.c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      struct stat s;
      if (stat((item + "/" + namelist[i]->d_name).c_str(), &s) < 0)
      {
         free(namelist[i]);
         continue;
      }

      if (S_ISDIR(s.st_mode))
         list_r((string(path) + "/" + namelist[i]->d_name).c_str(), filelist);
      else
         filelist.push_back(string(path) + "/" + namelist[i]->d_name);

      free(namelist[i]);
   }
   free(namelist);

   return filelist.size();
}

int Index2::lookup(const string& path, SNode& attr)
{
   return attr.deserialize2(m_strMetaPath + "/" + path);
}

int Index2::lookup(const string& path, set<Address, AddrComp>& addr)
{
   string item = m_strMetaPath + "/" + path;

   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) < 0)
      return SectorError::E_NOEXIST;

   if (!S_ISDIR(s.st_mode))
   {
      SNode sn;
      sn.deserialize2(m_strMetaPath + "/" + path);
      addr = sn.m_sLocation;
      return addr.size();
   }

   dirent **namelist;
   int n = scandir(item.c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      if (stat((item + "/" + namelist[i]->d_name).c_str(), &s) < 0)
      {
         free(namelist[i]);
         continue;
      }

      if (S_ISDIR(s.st_mode))
         lookup((string(path) + "/" + namelist[i]->d_name).c_str(), addr);
      else
      {
         SNode sn;
         sn.deserialize2(item + "/" + namelist[i]->d_name);
         for (set<Address, AddrComp>::const_iterator a = sn.m_sLocation.begin(); a != sn.m_sLocation.end(); ++ a)
            addr.insert(*a);
      }

      free(namelist[i]);
   }
   free(namelist);

   return addr.size();
}

int Index2::create(const SNode& node)
{
   struct stat s;
   if (stat((m_strMetaPath + "/" + node.m_strName).c_str(), &s) >= 0)
      return SectorError::E_EXIST;

   string tmp = node.m_strName;
   size_t p = tmp.rfind('/');
   if (p != string::npos)
   {
#ifndef WIN32
      string cmd = string("mkdir -p ") + m_strMetaPath + "/" + tmp.substr(0, p);
#else
      string cmd = string("mkdir \"") + m_strMetaPath + "/" + tmp.substr(0, p) + "\"";
#endif
      system(cmd.c_str());
   }

   if (node.m_bIsDir)
   {

#ifndef WIN32
      string cmd = string("mkdir ") + m_strMetaPath + "/" + node.m_strName;
#else
      string cmd = string("mkdir \"") + m_strMetaPath + "/" + node.m_strName + "\"";
#endif
      system(cmd.c_str());
   }
   else
   {
      string filename = tmp.substr(p + 1, tmp.length() - p - 1);
      string path = node.m_strName;
      SNode sn = node;
      sn.m_strName = filename;
      sn.serialize2(m_strMetaPath + "/" + path);
   }

   return 0;
}

int Index2::move(const string& oldpath, const string& newpath, const string& newname)
{
   vector<string> olddir;
   if (parsePath(oldpath, olddir) <= 0)
      return SectorError::E_NOEXIST;

   vector<string> newdir;
   if (parsePath(newpath, newdir) < 0)
      return SectorError::E_NOEXIST;

#ifndef WIN32
   string cmd ("mv "  + m_strMetaPath + "/" + oldpath + " " + m_strMetaPath + "/" + newpath + "/" + newname);
#else
   string cmd ("move /Y \""  + m_strMetaPath + "/" + oldpath + "\" \"" + m_strMetaPath + "/" + newpath + "/" + newname + "\"");
#endif
   system(cmd.c_str());

   return 0;
}

int Index2::remove(const string& path, bool recursive)
{
   vector<string> dir;
   if (parsePath(path, dir) <= 0)
      return SectorError::E_INVALID;

   if (dir.empty())
      return -1;

   string cmd;
   if (recursive)
   {
#ifndef WIN32
       cmd = string("rm -rf ") + m_strMetaPath + "/" + path;
#else
       string winpath(m_strMetaPath + "\\" + path);
       unix_to_win_path(winpath);
       cmd.append("del /F /Q /S \"").append(winpath).append("\"");
#endif
   }
   else
   {
#ifndef WIN32
       cmd = string("rm -f ") + m_strMetaPath + "/" + path;
#else
       string winpath(m_strMetaPath + "\\" + path);
       unix_to_win_path(winpath);
       cmd.append("del /F /Q \"").append(winpath).append("\"");
#endif
   }

   system(cmd.c_str());

   return 0;
}


int Index2::addReplica(const string& path, const int64_t& ts, const int64_t& size, const Address& addr)
{
   vector<string> dir;
   parsePath(path.c_str(), dir);

   if (dir.empty())
      return -1;

   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) >= 0)
   {
      SNode os;
      os.deserialize2(m_strMetaPath + "/" + path);
      if ((os.m_llSize != size) || (os.m_llTimeStamp != ts))
         return -1;

      os.m_sLocation.insert(addr);
      os.serialize2(m_strMetaPath + "/" + path);
      return 0;
   }

   return -1;
}

int Index2::removeReplica(const string& path, const Address& addr)
{
   vector<string> dir;
   parsePath(path.c_str(), dir);

   if (dir.empty())
      return -1;

   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) >= 0)
   {
      SNode os;
      if (os.deserialize2(m_strMetaPath + "/" + path) < 0)
         return -1;

      os.m_sLocation.erase(addr);
      os.serialize2(m_strMetaPath + "/" + path);
      return 0;
   }

   return -1;
}

int Index2::update(const string& path, const int64_t& ts, const int64_t& size)
{
   vector<string> dir;
   parsePath(path.c_str(), dir);

   if (dir.empty())
      return -1;

   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) >= 0)
   {
      SNode os;
      if (os.deserialize2(m_strMetaPath + "/" + path) < 0)
         return -1;

      if (size >= 0)
         os.m_llSize = size;

      os.m_llTimeStamp = ts;

      os.serialize2(m_strMetaPath + "/" + path);

      timeval t[2];
      t[0].tv_sec = ts;
      t[0].tv_usec = 0;
      t[1] = t[0];
      utimes((m_strMetaPath + "/" + path).c_str(), t); 

      return 0;
   }

   return -1;
}

int Index2::serialize(const string& path, const string& dstfile)
{
   ofstream ofs(dstfile.c_str());
   if (ofs.bad() || ofs.fail())
      return -1;

   serialize(ofs, path, 1);

   ofs.close();

   return 0;
}

int Index2::deserialize(const string& path, const string& srcfile, const Address* addr)
{
   vector<string> dirs;
   dirs.resize(1024);
   string currdir = m_strMetaPath;
   int currlevel = 1;

   ifstream ifs(srcfile.c_str());
   if (ifs.bad() || ifs.fail())
      return -1;

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
         sn.serialize2(currdir + "/" + sn.m_strName);
         dirs[level] = sn.m_strName;
      }
      else if (level == currlevel + 1)
      {
         currdir = currdir + "/" + dirs[currlevel];
         currlevel = level;

         sn.serialize2(currdir + "/" + sn.m_strName);
         dirs[level] = sn.m_strName;
      }
      else if (level < currlevel)
      {
         currdir = m_strMetaPath;

         for (int i = 1; i < level; ++ i)
            currdir = currdir + "/" + dirs[i];

         currlevel = level;

         sn.serialize2(currdir + "/" + sn.m_strName);
         dirs[level] = sn.m_strName;
      }
   }

   return 0;
}

int Index2::scan(const string& data, const string& meta)
{
   dirent **namelist;
   int n = scandir(data.c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

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
      {
         free(namelist[i]);
         continue;
      }

      struct stat s;
      if (stat((data + "/" + namelist[i]->d_name).c_str(), &s) < 0)
      {
         free(namelist[i]);
         continue;
      }

      // skip system file and directory
      if (S_ISDIR(s.st_mode) && (namelist[i]->d_name[0] == '.'))
      {
         free(namelist[i]);
         continue;
      }

      SNode sn;
      sn.m_strName = namelist[i]->d_name;

      sn.m_llSize = s.st_size;
      sn.m_llTimeStamp = s.st_mtime;

      if (S_ISDIR(s.st_mode))
      {
         sn.m_bIsDir = true;
#ifndef WIN32
         string cmd = string("mkdir -p ") +  m_strMetaPath + "/" + meta + "/" + namelist[i]->d_name;
#else
         string cmd = string("mkdir \"") +  m_strMetaPath + "/" + meta + "/" + namelist[i]->d_name + "\"";
#endif
         system(cmd.c_str());

         scan(data + "/" + namelist[i]->d_name, meta + "/" + namelist[i]->d_name);
      }
      else
      {
         sn.m_bIsDir = false;
         sn.serialize2(m_strMetaPath + "/" + meta + "/" + namelist[i]->d_name);
      }

      free(namelist[i]);
   }
   free(namelist);

   return 0;
}

int Index2::merge(const string& path, Metadata* meta, const unsigned int& replica)
{
   merge(path, ((Index2*)meta)->m_strMetaPath, replica);

   return 0;
}

int Index2::substract(const string& path, const Address& addr)
{
   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) < 0)
      return -1;

   if (!S_ISDIR(s.st_mode))
   {
      SNode sn;
      sn.deserialize2(m_strMetaPath + "/" + path);
      sn.m_sLocation.erase(addr);
      //if (sn.m_sLocation.empty())
      //   unlink(m_strMetaPath + path);
      sn.serialize2(m_strMetaPath + "/" + path);

      return 0;
   }

   dirent **namelist;
   int n = scandir((m_strMetaPath + "/" + path).c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      substract(path + "/" + namelist[i]->d_name, addr);

      free(namelist[i]);
   }
   free(namelist);

   return 0;
}

int64_t Index2::getTotalDataSize(const string& path)
{
   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) < 0)
      return -1;

   if (!S_ISDIR(s.st_mode))
   {
      SNode sn;
      sn.deserialize2(m_strMetaPath + "/" + path);
      return sn.m_llSize;
   }

   dirent **namelist;
   int n = scandir((m_strMetaPath + "/" + path).c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   int64_t size = 0;

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      size += getTotalDataSize(path + "/" + namelist[i]->d_name);

      free(namelist[i]);
   }
   free(namelist);

   return size;
}

int64_t Index2::getTotalFileNum(const string& path)
{
   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) < 0)
      return -1;

   if (!S_ISDIR(s.st_mode))
      return 1;

   dirent **namelist;
   int n = scandir((m_strMetaPath + "/" + path).c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   int64_t count = 0;

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      count += getTotalFileNum(path + "/" + namelist[i]->d_name);

      free(namelist[i]);
   }
   free(namelist);

   return count;
}

int Index2::collectDataInfo(const string& path, vector<string>& result)
{
   struct stat s;
   if (stat((m_strMetaPath + "/" + path).c_str(), &s) < 0)
      return -1;

   if (!S_ISDIR(s.st_mode))
   {
      // ignore index file
      int t = path.length();
      if ((t > 4) && (path.substr(t - 4, t) == ".idx"))
         return result.size();

      string idx = m_strMetaPath + "/" + path + ".idx";
      int rows = -1;
      SNode is;
      if (is.deserialize2(idx) >= 0)
         rows = static_cast<int>(is.m_llSize / 8 - 1);

      SNode sn;
      sn.deserialize2(m_strMetaPath + "/" + path);

      char buf[1024];
      sprintf(buf, "%s %lld %d", path.c_str(), (long long int)sn.m_llSize, rows);

      for (set<Address, AddrComp>::iterator i = sn.m_sLocation.begin(); i != sn.m_sLocation.end(); ++ i)
         sprintf(buf + strlen(buf), " %s %d", i->m_strIP.c_str(), i->m_iPort);

      result.insert(result.end(), buf);
      return result.size();
   }

   dirent **namelist;
   int n = scandir((m_strMetaPath + "/" + path).c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   for (int i = 0; i < n; ++ i)
   {
      // skip system files and directories
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      collectDataInfo((path + "/" + namelist[i]->d_name).c_str(), result);

      free(namelist[i]);
   }
   free(namelist);

   return result.size();
}

int Index2::checkReplica(const string& path, vector<string>& under, vector<string>& over, const unsigned int& thresh, const map<string, int>& special)
{
   dirent **namelist;
   int n = scandir((m_strMetaPath + "/" + path).c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return -1;

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      struct stat s, s_nosplit;
      if (stat((m_strMetaPath + "/" + path + "/" + namelist[i]->d_name).c_str(), &s) < 0)
         return -1;

      string abs_path = path + "/" + namelist[i]->d_name;

      if (!S_ISDIR(s.st_mode) || (stat((m_strMetaPath + "/" + abs_path + "/.nosplit").c_str(), &s_nosplit) >= 0))
      {
         SNode sn;
         sn.deserialize2(m_strMetaPath + "/" + abs_path);

         unsigned int d = thresh;
         for (map<string, int>::const_iterator s = special.begin(); s != special.end(); ++ s)
         {
            if (s->first.c_str()[s->first.length() - 1] == '/')
            {
               // check directory prefix
               if (abs_path.substr(0, s->first.length() - 1) + "/" == s->first)
               {
                  d = s->second;
                  break;
               }
            }
            else
            {
               // special file, name must match
               if (abs_path == s->first)
               {
                  d = s->second;
                  break;
               }
            }
         }

         if (stat((m_strMetaPath + "/" + abs_path + "/.nosplit").c_str(), &s_nosplit) >= 0)
            sn.deserialize2(m_strMetaPath + "/" + abs_path + "/.nosplit");

         if (sn.m_sLocation.size() < d)
            under.push_back(abs_path);
         else if (sn.m_sLocation.size() > d)
            over.push_back(abs_path);
      }
      else
         checkReplica(abs_path, under, over, thresh, special);

      free(namelist[i]);
   }
   free(namelist);

   return 0;
}

/////////////////////////////////////////////////////////////////////////////////

int Index2::serialize(ofstream& ofs, const string& currdir, int level)
{
   string item = m_strMetaPath + "/" + currdir;

   dirent **namelist;
   int n = scandir(item.c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      SNode sn;
      if (sn.deserialize2(item + "/" + namelist[i]->d_name) < 0)
         return -1;

      char* buf = new char[currdir.length() + 64];
      sn.serialize(buf);
      ofs << level << " " << buf << endl;
      delete [] buf;

      if (sn.m_bIsDir)
         serialize(ofs, currdir + "/" + namelist[i]->d_name, level + 1);

      free(namelist[i]);
   }
   free(namelist);

   return n;
}

int Index2::merge(const string& prefix, const string& path, const unsigned int& replica)
{
   dirent **namelist;
   int n = scandir(path.c_str(), &namelist, 0, alphasort);
   if (n < 0)
      return SectorError::E_NOEXIST;

   for (int i = 0; i < n; ++ i)
   {
      // skip system directory
      if (namelist[i]->d_name[0] == '.')
      {
         free(namelist[i]);
         continue;
      }

      struct stat s;
      if (stat((path + "/" + namelist[i]->d_name).c_str(), &s) < 0)
         continue;

      if (S_ISDIR(s.st_mode))
      {
         string dir = m_strMetaPath + "/" + prefix + "/" + namelist[i]->d_name;

         if (stat(dir.c_str(), &s) < 0)
         {
            //not exist
#ifndef WIN32
            string cmd = string("mv ") + path + "/" + namelist[i]->d_name + " " + m_strMetaPath + "/" + prefix;
#else
            string cmd = string("move /Y \"") + path + "/" + namelist[i]->d_name + "\" \"" + m_strMetaPath + "/" + prefix + "\"";
#endif
            system(cmd.c_str());
         }
         else
         {
            merge(prefix + "/" + namelist[i]->d_name, path + "/" + namelist[i]->d_name, replica);
         }
      }
      else
      {
         string file = m_strMetaPath + "/" + prefix + "/" + namelist[i]->d_name;

         if (stat(file.c_str(), &s) < 0)
         {
#ifndef WIN32
            string cmd = string("mv ") + path + "/" + namelist[i]->d_name + " " + m_strMetaPath + "/" + prefix;
#else
            string cmd = string("move /Y \"") + path + "/" + namelist[i]->d_name + "\" \"" + m_strMetaPath + "/" + prefix + "\"";
#endif
            system(cmd.c_str());
         }
         else
         {
            SNode os, ns;
            os.deserialize2(file);
            ns.deserialize2(path + "/" + namelist[i]->d_name);

            if ((os.m_llSize == ns.m_llSize) && (os.m_llTimeStamp == ns.m_llTimeStamp)) // && (os.m_sLocation.size() < replica))
            {
               // files with same name, size, timestamp
               // and the number of replicas is below the threshold
               for (set<Address, AddrComp>::const_iterator a = ns.m_sLocation.begin(); a != ns.m_sLocation.end(); ++ a)
                  os.m_sLocation.insert(*a);

               os.serialize2(file);
            }

            unlink((path + "/" + namelist[i]->d_name).c_str());
         }
      }

      free(namelist[i]);
   }
   free(namelist);

   return 0;
}
