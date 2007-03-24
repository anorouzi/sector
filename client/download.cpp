#ifdef WIN32
   #include <windows.h>
#else
   #include <unistd.h>
   #include <sys/ioctl.h>
   #include <termios.h>
#endif

#include <fstream>
#include <fsclient.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

using namespace std;
using namespace cb;

int download(CFSClient& fsclient, const char* file, const char* dest)
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

   CFileAttr attr;
   if (fsclient.stat(file, attr) < 0)
   {
      cout << "ERROR: cannot locate file " << file << endl;
      return -1;
   }

   long long int size = attr.m_llSize;
   cout << "downloading " << file << " of " << size << " bytes" << endl;

   //CProgressBar bar;
   //bar.init(size);

   CCBFile* fh = fsclient.createFileHandle();
   if (NULL == fh)
      return -1;
   if (fh->open(file) < 0)
   {
      cout << "unable to locate file" << endl;
      return -1;
   }

   string localpath;
   if (dest[strlen(dest) - 1] != '/')
      localpath = string(dest) + string("/") + string(file);
   else
      localpath = string(dest) + string(file);

   bool finish = true;
   if (fh->download(localpath.c_str(), true) < 0)
      finish = false;

   fh->close();
   fsclient.releaseFileHandle(fh);

   if (finish)
   {
      #ifndef WIN32
         gettimeofday(&t2, 0);
         float throughput = size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);
      #else
         float throughput = size * 8.0 / 1000000.0 / ((GetTickCount() - t1) / 1000.0);
      #endif

      //bar.update(size, throughput);

      cout << "Downloading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;

      return 1;
   }

   return -1;
}

int main(int argc, char** argv)
{
   vector<string> filelist;
   ifstream src(argv[3]);
   char buf[1024];

   while (!src.eof())
   {
      src.getline(buf, 1024);
      if (0 != strlen(buf))
         filelist.insert(filelist.end(), buf);
   }
   src.close();

   CFSClient fsclient;
   if (-1 == fsclient.connect(argv[1], atoi(argv[2])))
   {
      cout << "unable to connect to the server at " << argv[1] << endl;
      return -1;
   }

   for (vector<string>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      int c = 0;

      for (; c < 5; ++ c)
      {
         if (download(fsclient, i->c_str(), argv[4]) > 0)
            break;
         else if (c < 4)
            cout << "download interuppted, trying again from break point." << endl;
         else
            cout << "download failed after 5 attempts." << endl;
      }
   }

   fsclient.close();

   return 1;
}
