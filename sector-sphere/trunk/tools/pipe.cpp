#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <sector.h>
#include <conf.h>

using namespace std;

int upload(const char* file, const char* dst, Sector& client)
{
   timeval t1, t2;
   gettimeofday(&t1, 0);

   struct stat64 s;
   stat64(file, &s);
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
      gettimeofday(&t2, 0);
      float throughput = s.st_size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

      cout << "Uploading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
   }
   else
      cout << "Uploading failed! Please retry. " << endl << endl;

   return 1;
}

int main(int argc, char** argv)
{
   if (2 != argc)
   {
      cout << "usage: sector_pipe dst_file" << endl;
      return 0;
   }

   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   if (client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;


   timeval t1, t2;
   gettimeofday(&t1, 0);

   SectorFile* f = client.createSectorFile();

   if (f->open(argv[1], SF_MODE::WRITE | SF_MODE::APPEND) < 0)
   {
      cout << "ERROR: unable to open destination file." << endl;
      return -1;
   }

   int size = 1000000;
   char* buf = new char[size];
   int read_size = size;
   int64_t total_size = 0;

   while(true)
   {
      read_size = read(0, buf, size);
      if (read_size <= 0)
         break;
      f->write(buf, read_size);
      total_size += read_size;
   }

   f->close();
   client.releaseSectorFile(f);

   gettimeofday(&t2, 0);
   float throughput = total_size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

   cout << "Writing accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;

   client.logout();
   client.close();

   return 1;
}
