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

#include <sys/types.h>
#include <sys/stat.h>
#ifndef WIN32
    #include <sys/time.h>
    #include <unistd.h>
#else
#include "common.h"
#endif
#include <stdio.h>
#include <iostream>
#include <sector.h>
#include <conf.h>

using namespace std;

void print_error(int code)
{
   cerr << "ERROR: " << code << " " << SectorError::getErrorMsg(code) << endl;
}

int main(int argc, char** argv)
{
   CmdLineParser clp;
   if ((clp.parse(argc, argv) < 0) || (clp.m_mDFlags.size() != 1))
   {
      cerr << "usage #1: <your_application> | sector_pipe -d dst_file" << endl;
      cerr << "usage #2: sector_pipe -s src_file | <your_application>" << endl;
      return 0;
   }

   string option = clp.m_mDFlags.begin()->first;

   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   int result = 0;
   if ((result = client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort)) < 0)
   {
      print_error(result);
      return -1;
   }
   if ((result = client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str())) < 0)
   {
      print_error(result);
      return -1;
   }

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
#ifndef WIN32
         read_size = read(0, buf, size);
#else
          // TODO
         read_size = -1;
#endif
         if (read_size <= 0)
             break;
         f->write(buf, read_size);
         total_size += read_size;
      }

      delete [] buf;
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
         read_size = static_cast<int>(f->read(buf, size));
         if (read_size <= 0)
             break;
         total_size += read_size;
         buf[read_size + 1] = 0;
         printf("%s", buf);
      }

      delete [] buf;
   }

   f->close();
   client.releaseSectorFile(f);

   gettimeofday(&t2, 0);
   float throughput = total_size * 8.0f / 1000000.0f / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0f);

   cerr << "Pipeline accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;

   client.logout();
   client.close();

   return 0;
}
