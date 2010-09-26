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
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/

#ifdef WIN32
   #include <windows.h>
   #include "dirent.h"
   #include "statfs.h"
#else
   #include <unistd.h>
   #include <sys/ioctl.h>
   #include <sys/time.h>
#endif

#include <fstream>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#ifndef WIN32
    #include <sys/vfs.h>
    #include <sys/statvfs.h>
#endif
#include <iostream>
#include "sector.h"
#include "conf.h"
#include <utility.h>

#include "common.h"

using namespace std;

int download(const char* file, const char* dest, Sector& client)
{
   CTimer timer;
   uint64_t t1 = timer.getTime();  // returns time in microseconds (usecs)

   SNode attr;
   if (client.stat(file, attr) < 0)
   {
      cerr << "ERROR: cannot locate file " << file << endl;
      return -1;
   }

   if (attr.m_bIsDir)
   {
      ::mkdir((string(dest) + "/" + file).c_str(), S_IRWXU);
      return 1;
   }

   long long int size = attr.m_llSize;
   cout << "downloading " << file << " of " << size << " bytes" << endl;

   SectorFile* f = client.createSectorFile();

   if (f->open(file) < 0)
   {
      cerr << "unable to locate file " << file << endl;
      return -1;
   }

   int sn = strlen(file) - 1;
   for (; sn >= 0; sn --)
   {
      if (file[sn] == '/')
         break;
   }
   string localpath;
   if (dest[strlen(dest) - 1] != '/')
      localpath = string(dest) + string("/") + string(file + sn + 1);
   else
      localpath = string(dest) + string(file + sn + 1);

   int64_t result = f->download(localpath.c_str(), true);

   f->close();
   client.releaseSectorFile(f);

   if (result >= 0)
   {
      float throughput = size * 8.0f / 1000000.0f / ((timer.getTime() - t1) / 1000000.0f);

      cout << "Downloading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;

      return 0;
   }

   cerr << "error happened during downloading " << file << endl;
   Utility::print_error(static_cast<int>(result));

   return -1;
}

int getFileList(const string& path, vector<string>& fl, Sector& client)
{
   SNode attr;
   if (client.stat(path.c_str(), attr) < 0)
      return -1;

   fl.push_back(path);

   if (attr.m_bIsDir)
   {
      vector<SNode> subdir;
      client.list(path, subdir);

      for (vector<SNode>::iterator i = subdir.begin(); i != subdir.end(); ++ i)
      {
         if (i->m_bIsDir)
            getFileList(path + "/" + i->m_strName, fl, client);
         else
            fl.push_back(path + "/" + i->m_strName);
      }
   }

   return fl.size();
}

int main(int argc, char** argv)
{
   if (argc != 3)
   {
      cerr << "USAGE: download <src file/dir> <local dir>\n";
      return -1;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return 0;

   vector<string> fl;
   bool wc = WildCard::isWildCard(argv[1]);
   if (!wc)
   {
      SNode attr;
      if (client.stat(argv[1], attr) < 0)
      {
         cerr << "ERROR: source file does not exist.\n";
         return -1;
      }
      getFileList(argv[1], fl, client);
   }
   else
   {
      string path = argv[1];
      string orig = path;
      size_t p = path.rfind('/');
      if (p == string::npos)
         path = "/";
      else
      {
         path = path.substr(0, p);
         orig = orig.substr(p + 1, orig.length() - p);
      }

      vector<SNode> filelist;
      int r = client.list(path, filelist);
      if (r < 0)
         cerr << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

      for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      {
         if (WildCard::match(orig, i->m_strName))
            getFileList(path + "/" + i->m_strName, fl, client);
      }
   }

   string newdir(argv[2]);
#ifdef WIN32
    win_to_unix_path (newdir);
#endif
   struct stat st;
   int r = stat(newdir.c_str(), &st);
   if ((r < 0) || !S_ISDIR(st.st_mode))
   {
      cerr << "ERROR: destination directory does not exist.\n";
      return -1;
   }


   string olddir;
   for (int i = strlen(argv[1]) - 1; i >= 0; -- i)
   {
      if (argv[1][i] != '/')
      {
         olddir = string(argv[1]).substr(0, i);
         break;
      }
   }
   size_t p = olddir.rfind('/');
   if (p == string::npos)
      olddir = "";
   else
      olddir = olddir.substr(0, p);


   for (vector<string>::iterator i = fl.begin(); i != fl.end(); ++ i)
   {
      string dst = *i;
      if (olddir.length() > 0)
         dst.replace(0, olddir.length(), newdir);
      else
         dst = newdir + "/" + dst;

      string localdir = dst.substr(0, dst.rfind('/'));

      // if localdir does not exist, create it
      if (stat(localdir.c_str(), &st) < 0)
      {
         for (unsigned int p = 1; p < localdir.length(); ++ p)
         {
            if (localdir.c_str()[p] == '/')
            {
               string substr = localdir.substr(0, p);

               if ((-1 == ::mkdir(substr.c_str(), S_IRWXU)) && (errno != EEXIST))
               {
                  cerr << "ERROR: unable to create local directory " << substr << endl;
                  return -1;
               }
            }
         }

         if ((-1 == ::mkdir(localdir.c_str(), S_IRWXU)) && (errno != EEXIST))
         {
            cerr << "ERROR: unable to create local directory " << localdir << endl;
            break;
         }
      }

      if (download(i->c_str(), localdir.c_str(), client) < 0)
      {
         // calculate total available disk size
         struct statvfs dstinfo;
         statvfs(newdir.c_str(), &dstinfo);
         int64_t availdisk = dstinfo.f_bavail * dstinfo.f_bsize;

         if (availdisk <= 0)
         {
            cerr << "insufficient local disk space. quit.\n";
            break;
         }
      }
   }

   Utility::logout(client);

   return 0;
}
