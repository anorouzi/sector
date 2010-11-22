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

using namespace std;

void help()
{
   cerr << "usage: sector_upload <src file/dir> <dst dir> [-n num_of_replicas] [-a ip_address] [-c cluster_id] [--e(ncryption)]" << endl;
}

int upload(const char* file, const char* dst, Sector& client, const int rep_num, const string& ip, const string& cid, const bool secure)
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

   SF_OPT option;
   option.m_llReservedSize = s.st_size;
   if (option.m_llReservedSize <= 0)
      option.m_llReservedSize = 1;
   option.m_iReplicaNum = rep_num;
   option.m_strHintIP = ip;
   option.m_strCluster = cid;

   int mode = SF_MODE::WRITE;
   if (secure)
      mode |= SF_MODE::SECURE;

   int r = f->open(dst, mode, &option);
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
      return result;
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

   int replica_num = 1;
   string ip = "";
   string cluster = "";

   bool encryption = false;

   for (map<string, string>::const_iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "n")
         replica_num = atoi(i->second.c_str());
      else if (i->first == "a")
         ip = i->second;
      else if (i->first == "c")
         cluster = i->second;
      else
      {
         help();
         return -1;
      }
   }

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

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   string newdir = *clp.m_vParams.rbegin();
   SNode attr;
   int r = client.stat(newdir, attr);
   if ((r < 0) || (!attr.m_bIsDir))
   {
      cerr << "destination directory on Sector does not exist.\n";
      Utility::logout(client);
      return -1;
   }

   bool success = true;

   clp.m_vParams.erase(clp.m_vParams.begin() + clp.m_vParams.size() - 1);
   for (vector<string>::iterator i = clp.m_vParams.begin(); i < clp.m_vParams.end(); ++ i)
   {
      vector<string> fl;
      bool wc = WildCard::isWildCard(*i);
      if (!wc)
      {
         struct stat64 st;
         if (stat64(i->c_str(), &st) < 0)
         {
            cerr << "ERROR: source file does not exist.\n";
            return -1;
         }
         getFileList(*i, fl);
      }
      else
      {
         string path = *i;
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
            if (upload(i->c_str(), dst.c_str(), client, replica_num, ip, cluster, encryption) < 0)
               success = false;
         }
      }
   }

   Utility::logout(client);

   return success ? 0 : -1;
}
