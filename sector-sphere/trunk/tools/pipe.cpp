#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <sector.h>
#include <conf.h>

using namespace std;

int main(int argc, char** argv)
{
   CmdLineParser clp;
   if ((clp.parse(argc, argv) <= 0) || (clp.m_mParams.size() != 1))
   {
      cerr << "usage #1: <your_application> | sector_pipe -d dst_file" << endl;
      cerr << "usage #2: sector_pipe -s src_file | <your_application>" << endl;
      return 0;
   }

   string option = clp.m_mParams.begin()->first;

   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   if (client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;


   timeval t1, t2;
   gettimeofday(&t1, 0);
   int64_t total_size = 0;

   SectorFile* f = client.createSectorFile();

   if (option == "d")
   {
      if (f->open(argv[2], SF_MODE::WRITE | SF_MODE::APPEND) < 0)
      {
         cerr << "ERROR: unable to open destination file." << endl;
         return -1;
      }

      int size = 1000000;
      char* buf = new char[size];
      int read_size = size;

      while(true)
      {
         read_size = read(0, buf, size);
         if (read_size <= 0)
             break;
         f->write(buf, read_size);
         total_size += read_size;
      }
   }
   else if (option == "s")
   {
      if (f->open(argv[2], SF_MODE::READ) < 0)
      {
         cerr << "ERROR: unable to open destination file." << endl;
         return -1;
      }

      int size = 1000000;
      char* buf = new char[size + 1];
      int read_size = size;

      while(!f->eof())
      {
         read_size = f->read(buf, size);
         if (read_size <= 0)
             break;
         total_size += read_size;
         buf[read_size + 1] = 0;
         printf("%s", buf);
      }
   }

   f->close();
   client.releaseSectorFile(f);

   gettimeofday(&t2, 0);
   float throughput = total_size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

   cerr << "Pipeline accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;

   client.logout();
   client.close();

   return 0;
}
