#include <fcntl.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <iostream>

#include <fsclient.h>

using namespace std;

int upload(const char* file, const char* dst)
{
   timeval t1, t2;
   gettimeofday(&t1, 0);

   ifstream ifs(file);
   ifs.seekg(0, ios::end);
   long long int size = ifs.tellg();
   ifs.seekg(0);
   cout << "uploading " << file << " of " << size << " bytes" << endl;

   SectorFile f;

   if (f.open(dst, SF_MODE::WRITE) < 0)
   {
      cout << "ERROR: unable to connect to server or file already exists." << endl;
      return -1;
   }

   bool finish = true;
   if (f.upload(file) < 0)
      finish = false;

   f.close();

   if (finish)
   {
      gettimeofday(&t2, 0);
      float throughput = size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

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
   if (5 != argc)
   {
      cout << "usage: upload <ip> <port> <src file/dir> <dst dir>" << endl;
      return 0;
   }

   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   vector<string> fl;
   getFileList(argv[3], fl);

   string olddir;
   for (int i = strlen(argv[3]) - 1; i >= 0; -- i)
   {
      if (argv[3][i] != '/')
      {
         olddir = string(argv[3]).substr(0, i);
         break;
      }
   }
   size_t p = olddir.rfind('/');
   if (p == string::npos)
      olddir = "";
   else
      olddir = olddir.substr(0, p);

   string newdir = argv[4];
   SNode attr;
   int r = Sector::stat(newdir, attr);
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
         Sector::mkdir(dst);
      else
         upload(i->c_str(), dst.c_str());
   }

   Sector::logout();
   Sector::close();

   return 1;
}
