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
#else
   #include <unistd.h>
   #include <sys/ioctl.h>
#endif

#include <fstream>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <iostream>
#include <sector.h>

using namespace std;

void help()
{
   cout << "download sector_file/dir local_dir [--e]" << endl;
}

int download(const char* file, const char* dest, Sector& client, bool encryption)
{
   #ifndef WIN32
      timeval t1, t2;
   #else
      DWORD t1, t2;
   #endif

   #ifndef WIN32
      gettimeofday(&t1, 0);
   #else
      t1 = GetTickCount();
   #endif

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

   int mode = SF_MODE::READ;
   if (encryption)
      mode |= SF_MODE::SECURE;

   if (f->open(file, mode) < 0)
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
      #ifndef WIN32
         gettimeofday(&t2, 0);
         float throughput = size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);
      #else
         float throughput = size * 8.0 / 1000000.0 / ((GetTickCount() - t1) / 1000.0);
      #endif

      cout << "Downloading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;

      return 0;
   }

   cerr << "error happened during downloading " << file << endl;
   Utility::print_error(result);

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
   if (argc < 3)
   {
      help();
      return -1;
   }

   CmdLineParser clp;
   if (clp.parse(argc, argv) < 0)
   {
      help();
      return -1;
   }

   if (clp.m_vParams.size() < 2)
   {
      help();
      return -1;
   }

   bool encryption = false;

   for (vector<string>::const_iterator i = clp.m_vSFlags.begin(); i != clp.m_vSFlags.end(); ++ i)
   {
      if (*i == "e")
         encryption = true;
      else
      {
         help();
         return -1;
      }
   }

   string newdir = *clp.m_vParams.rbegin();
   clp.m_vParams.erase(clp.m_vParams.begin() + clp.m_vParams.size() - 1);

   // check destination directory, which must exist
   struct stat64 st;
   int r = stat64(newdir.c_str(), &st);
   if ((r < 0) || !S_ISDIR(st.st_mode))
   {
      cerr << "ERROR: destination directory does not exist.\n";
      return -1;
   }

   // login to SectorFS
   Sector client;
   if (Utility::login(client) < 0)
      return 0;

   // start downloading all files
   for (vector<string>::iterator i = clp.m_vParams.begin(); i != clp.m_vParams.end(); ++ i)
   {
      vector<string> fl;
      bool wc = WildCard::isWildCard(*i);
      if (!wc)
      {
         SNode attr;
         if (client.stat(*i, attr) < 0)
         {
            cerr << "ERROR: source file does not exist.\n";
            return -1;
         }
         getFileList(*i, fl, client);
      }
      else
      {
         string path = *i;
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

      string olddir;
      for (int j = i->length() - 1; j >= 0; -- j)
      {
         if (i->c_str()[j] != '/')
         {
            olddir = i->substr(0, j);
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
         if (stat64(localdir.c_str(), &st) < 0)
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

         if (download(i->c_str(), localdir.c_str(), client, encryption) < 0)
         {
            // calculate total available disk size
            struct statvfs64 dstinfo;
            statvfs64(newdir.c_str(), &dstinfo);
            int64_t availdisk = dstinfo.f_bavail * dstinfo.f_bsize;

            if (availdisk <= 0)
            {
               // if no disk space svailable, no need to try any more
               cerr << "insufficient local disk space. quit.\n";
               Utility::logout(client);
               return -1;
            }
         }
      }
   }

   Utility::logout(client);

   return 0;
}
