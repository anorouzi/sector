#include <fstream>
#include <fsclient.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <string.h>
#include <errno.h>

using namespace std;
using namespace cb;

int upload(CFSClient& fsclient, const char* file, const char* dst = NULL)
{
   timeval t1, t2;
   gettimeofday(&t1, 0);

   ifstream ifs(file);
   ifs.seekg(0, ios::end);
   long long int size = ifs.tellg();
   ifs.seekg(0);
   cout << "uploading " << file << " of " << size << " bytes" << endl;

   //CProgressBar bar;
   //bar.init(size);

   CCBFile* fh = fsclient.createFileHandle();
   if (NULL == fh)
      return -1;

   char* rname;

   if (NULL != dst)
   {
      rname = (char*)dst;
   }
   else
   {
      rname = (char*)file;
      for (int i = strlen(file); i >= 0; -- i)
      {
         if ('/' == file[i])
         {
            rname = (char*)file + i + 1;
            break;
         }
      }
   }

   char cert[1024];
   cert[0] = '\0';
   if (fh->open(rname, 2, cert) < 0)
   {
      cout << "ERROR: unable to connect to server." << endl;
      return -1;
   }

   if (0 != strlen(cert))
   {
      cout << "file owner certificate: " << cert << endl;

      ofstream ofs((string(rname) + ".cert").c_str());
      ofs << cert << endl;
      ofs.close();
   }

   bool finish = true;
   if (fh->upload(file) < 0)
      finish = false;

   fh->close();
   fsclient.releaseFileHandle(fh);

   if (finish)
   {
      gettimeofday(&t2, 0);
      float throughput = size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

      //bar.update(size, throughput);

      cout << "Uploading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
   }
   else
      cout << "Uploading failed! Please retry. " << endl << endl;

   return 1;
}

int main(int argc, char** argv)
{
   if ((4 != argc) && (5 != argc))
   {
      cout << "usage: upload <ip> <port> <src file> [dst file]" << endl;
      return 0;
   }

   CFSClient fsclient;
   fsclient.connect(argv[1], atoi(argv[2]));

   if (5 == argc)
      upload(fsclient, argv[3], argv[4]);
   else
      upload(fsclient, argv[3]);

   fsclient.close();

   return 1;
}
