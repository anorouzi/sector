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
   Yunhong Gu, last updated 09/15/2010
*****************************************************************************/

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <iostream>
#include <sector.h>
#include <conf.h>
#include <utility.h>

using namespace std;

int upload(const char* file, const char* dst, Sector& client)
{
   //check if file already exists

   struct stat64 st;
   if (stat64(file, &st) < 0)
   {
      cout << "cannot locate source file " << file << endl;
      return -1;
   }

   SNode attr;
   if (client.stat(dst, attr) >= 0)
   {
      if (attr.m_llSize == st.st_size)
      {
         cout << "destination file " << dst << " exists on Sector FS. skip.\n";
         return 0;
      }
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);

   struct stat64 s;
   stat64(file, &s);
   cout << "uploading " << file << " of " << s.st_size << " bytes" << endl;

   SectorFile* f = client.createSectorFile();

   int r = f->open(dst, SF_MODE::WRITE, "", s.st_size);
   if (r < 0)
   {
      cerr << "unable to open file " << dst << endl;
      Utility::print_error(r);
      return -1;
   }

   int64_t result = f->upload(file);

   f->close();
   client.releaseSectorFile(f);

   if (result >= 0)
   {
      gettimeofday(&t2, 0);
      float throughput = s.st_size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

      cout << "Uploading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
   }
   else
   {
      cout << "Uploading failed! Please retry. " << endl << endl;
      Utility::print_error(result);
      return -1;
   }

   return 0;
}

int getFileList(const string& path, vector<string>& fl)
{
   fl.push_back(path);

   struct stat64 s;
   stat64(path.c_str(), &s);

   if (S_ISDIR(s.st_mode))
   {
      // if there is a ".nosplit" file, must upload this file the first in the directory, subsequent files will be uploaded to the same node
      if (stat64((path + "/.nosplit").c_str(), &s) > 0)
         fl.push_back(path + "/.nosplit");

      dirent **namelist;
      int n = scandir(path.c_str(), &namelist, 0, alphasort);

      if (n < 0)
         return -1;

      for (int i = 0; i < n; ++ i)
      {
         // skip "." and ".." and hidden directory
         if (namelist[i]->d_name[0] == '.')
         {
            free(namelist[i]);
            continue;
         }

         string subdir = path + "/" + namelist[i]->d_name;

         if (stat64(subdir.c_str(), &s) < 0)
            continue;

         if (S_ISDIR(s.st_mode))
            getFileList(subdir, fl);
         else
            fl.push_back(subdir);
      }
   }

   return fl.size();
}

int main(int argc, char** argv)
{
   if (argc < 3)
   {
      cerr << "usage: upload <src file/dir> <dst dir>" << endl;
      return 0;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   string newdir = argv[argc - 1];
   SNode attr;
   int r = client.stat(newdir, attr);
   if ((r < 0) || (!attr.m_bIsDir))
   {
      cerr << "destination directory on Sector does not exist.\n";
      Utility::logout(client);
      return -1;
   }

   bool success = true;

   for (int i = 1; i < argc - 1; ++ i)
   {
      vector<string> fl;
      bool wc = WildCard::isWildCard(argv[i]);
      if (!wc)
      {
         struct stat64 st;
         if (stat64(argv[i], &st) < 0)
         {
            cerr << "ERROR: source file does not exist.\n";
            return -1;
         }
         getFileList(argv[i], fl);
      }
      else
      {
         string path = argv[i];
         string orig = path;
         size_t p = path.rfind('/');
         if (p == string::npos)
         {
            path = ".";
         }
         else
         {
            path = path.substr(0, p);
            orig = orig.substr(p + 1, orig.length() - p);
         }

         dirent **namelist;
         int n = scandir(path.c_str(), &namelist, 0, alphasort);

         if (n < 0)
            return -1;

         for (int i = 0; i < n; ++ i)
         {
            // skip "." and ".." and hidden directory
            if (namelist[i]->d_name[0] == '.')
            {
               free(namelist[i]);
               continue;
            }

            if (WildCard::match(orig, namelist[i]->d_name))
            {
               if (path == ".")
                  getFileList(namelist[i]->d_name, fl);
               else
                  getFileList(path + "/" + namelist[i]->d_name, fl);
            }
         }
      }

      string olddir;
      for (int j = strlen(argv[i]) - 1; j >= 0; -- j)
      {
         if (argv[i][j] != '/')
         {
            olddir = string(argv[i]).substr(0, j);
            break;
         }
      }
      size_t p = olddir.rfind('/');
      if (p == string::npos)
         olddir = "";
      else
         olddir = olddir.substr(0, p);

      for (vector<string>::const_iterator i = fl.begin(); i != fl.end(); ++ i)
      {
         string dst = *i;
         if (olddir.length() > 0)
            dst.replace(0, olddir.length(), newdir);
         else
            dst = newdir + "/" + dst;

         struct stat64 s;
         if (stat64(i->c_str(), &s) < 0)
            continue;

         if (S_ISDIR(s.st_mode))
            client.mkdir(dst);
         else
         {
            if (upload(i->c_str(), dst.c_str(), client) < 0)
               success = false;
         }
      }
   }

   Utility::logout(client);

   return success ? 0 : -1;
}
