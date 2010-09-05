#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <iostream>
#include <sector.h>
#include <conf.h>
#ifndef WIN32
    #include <unistd.h>
    #include <sys/ioctl.h>
    #include <sys/time.h>
#else
    #include "statfs.h"
#endif

#include "common.h"


using namespace std;

int upload(const char* file, const char* dst, Sector& client)
{
   CTimer timer;
   uint64_t t1 = timer.getTime();  // returns time in microseconds (usecs)

   struct stat s;
   stat(file, &s);
   cout << "uploading " << file << " of " << s.st_size << " bytes" << endl;

   SectorFile* f = client.createSectorFile();

   if (f->open(dst, SF_MODE::WRITE) < 0)
   {
      cout << "ERROR: unable to connect to server or file already exists." << endl;
      return -1;
   }

   bool finish = true;
   if (f->upload(file) < 0LL)
      finish = false;

   f->close();
   client.releaseSectorFile(f);

   if (finish)
   {
      float throughput = s.st_size * 8.0f / 1000000.0f / ((timer.getTime() - t1) / 1000000.0f);

      cout << "Uploading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
   }
   else
      cout << "Uploading failed! Please retry. " << endl << endl;

   return 1;
}

int getFileList(const string& path, vector<string>& fl)
{
   fl.push_back(path);

   struct stat s;
   stat(path.c_str(), &s);

   if (S_ISDIR(s.st_mode))
   {
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

         if (stat(subdir.c_str(), &s) < 0)
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
   if (3 != argc)
   {
      cout << "usage: upload <src file/dir> <dst dir>" << endl;
      return 0;
   }

   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   if (client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;


   vector<string> fl;
   string path = argv[1];
#ifdef WIN32
    win_to_unix_path (path);
#endif
   bool wc = WildCard::isWildCard(path.c_str());
   if (!wc)
   {
      struct stat st;
      if (stat(path.c_str(), &st) < 0)
      {
         cout << "ERROR: source file does not exist.\n";
         return -1;
      }
      getFileList(path.c_str(), fl);
   }
   else
   {
      string orig = path;
      size_t p = path.rfind('/');
      if (p == string::npos)
         path = "/";
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
            getFileList(path + "/" + namelist[i]->d_name, fl);
      }
   }


   string olddir;
   string input_path = argv[1];
#ifdef WIN32
    win_to_unix_path (input_path);
#endif
   for (int i = input_path.length() - 1; i >= 0; -- i)
   {
      if (input_path[i] != '/')
      {
         olddir = input_path.substr(0, i);
         break;
      }
   }
   size_t p = olddir.rfind('/');
   if (p == string::npos)
      olddir = "";
   else
      olddir = olddir.substr(0, p);

   string newdir = argv[2];
   SNode attr;
   int r = client.stat(newdir, attr);
   if ((r < 0) || (!attr.m_bIsDir))
   {
      cout << "destination directory on Sector does not exist.\n";
      return -1;
   }

   for (vector<string>::const_iterator i = fl.begin(); i != fl.end(); ++ i)
   {
      string dst = *i;
      if (olddir.length() > 0)
         dst.replace(0, olddir.length(), newdir);
      else
         dst = newdir + "/" + dst;

      struct stat s;
      if (stat(i->c_str(), &s) < 0)
         continue;

      if (S_ISDIR(s.st_mode))
         client.mkdir(dst);
      else
         upload(i->c_str(), dst.c_str(), client);
   }

   client.logout();
   client.close();

   return 1;
}
