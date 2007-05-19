/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 03/24/2007
*****************************************************************************/

#include <server.h>
#include <util.h>
#include <dlfcn.h>
#include <fsclient.h>

using namespace cb;

void* Server::SPEHandler(void* p)
{
   Server* self = ((Param4*)p)->s;
   UDTSOCKET u = ((Param4*)p)->u;
   string ip = ((Param4*)p)->ip;
   int port = ((Param4*)p)->port;
   int uport = ((Param4*)p)->p;
   int speid = ((Param4*)p)->id;
   string op = ((Param4*)p)->op;
   string opara = ((Param4*)p)->param;
   delete (Param4*)p;
   CCBMsg msg;

   sockaddr_in cli_addr;
   cli_addr.sin_family = AF_INET;
   cli_addr.sin_port = uport;
   inet_pton(AF_INET, ip.c_str(), &cli_addr.sin_addr);
   memset(&(cli_addr.sin_zero), '\0', 8);

   cout << "rendezvous connect " << ip << " " << uport << endl;

   if (UDT::ERROR == UDT::connect(u, (sockaddr*)&cli_addr, sizeof(sockaddr_in)))
      return NULL;

   cout << "locating so " << (self->m_strHomeDir + op + ".so") << endl;

   void* handle = dlopen((self->m_strHomeDir + op + ".so").c_str(), RTLD_LAZY);
   if (NULL == handle)
      return NULL;

   cout << "so found " << "locating process " << op << endl;

   int (*process)(const char*, const int&, char*, int&, const char*, const int&);
   process = (int (*) (const char*, const int&, char*, int&, const char*, const int&) )dlsym(handle, op.c_str());
   if (NULL == process)
   {
      cout << dlerror() <<  endl;
      return NULL;
   }

   cout << "process found~\n";

   timeval t1, t2, t3, t4;
   gettimeofday(&t1, 0);

   string datafile;
   int64_t offset = 0;
   int64_t rows = 0;
   int64_t* index = NULL;
   int size = 0;
   char* block = NULL;
   char* res = NULL;
   int rsize = 0;
   int rs;
   int progress;

   msg.setType(1); // success, return result
   msg.setData(0, (char*)&(speid), 4);

   // processing...
   while (true)
   {
      char param[80];
      if (UDT::recv(u, param, 80, 0) < 0)
         break;

      datafile = param;
      offset = *(int64_t*)(param + 64);
      rows = *(int64_t*)(param + 72);

      cout << "new job " << datafile << " " << offset << " " << rows << endl;

      // read data
      if (self->m_LocalFile.lookup(datafile.c_str(), NULL) > 0)
      {
         index = new int64_t[rows + 1];
         ifstream idx;
         idx.open((self->m_strHomeDir + datafile + ".idx").c_str());
         idx.seekg(offset * 8);
         idx.read((char*)index, (rows + 1) * 8);
         idx.close();

         size = index[rows] - index[0];
         cout << "to read data " << size << endl;
         block = new char[size];
         res = new char[size];

         ifstream ifs;
         ifs.open((self->m_strHomeDir + datafile).c_str());
         ifs.seekg(index[0]);
         ifs.read(block, size);
         ifs.close();

         cout << "read data into block...\n";
      }
      else
      {
         File* f = Client::createFileHandle();
         f->open(datafile.c_str());
         f->readridx((char*)index, offset, rows);

         size = index[rows] - index[0];
         block = new char[size];
         res = new char[size];

         f->read(block, index[0], size);

         f->close();
         Client::releaseFileHandle(f);
      }

      rsize = 0;
      gettimeofday(&t3, 0);
      for (int i = 0; i < rows; ++ i)
      {
         //cout << "to process " << index[i] - index[0] << " " << index[i + 1] - index[i] << endl;
         process(block + index[i] - index[0], index[i + 1] - index[i], res + rsize, rs, opara.c_str(), opara.length());
         rsize += rs;
         rs = size - rsize;

         gettimeofday(&t4, 0);
         if (t4.tv_sec - t3.tv_sec > 1)
         {
            progress = i * 100 / rows;
            msg.setData(4, (char*)&progress, 4);
            msg.m_iDataLength = 4 + 8;
            if (self->m_GMP.rpc(ip.c_str(), port, &msg, &msg) < 0)
               return NULL;
            t3 = t4;
         }
      }

      progress = 100;
      msg.setData(4, (char*)&progress, 4);
      msg.setData(8, (char*)&rsize, 4);
      msg.m_iDataLength = 4 + 12;
      if (self->m_GMP.rpc(ip.c_str(), port, &msg, &msg) < 0)
         return NULL;

      cout << "sending data back... " << rsize << " " << *(int*)res << endl;

      int h;
      if (UDT::ERROR == UDT::send(u, res, rsize, 0, &h))
      {
         cout << UDT::getlasterror().getErrorMessage() << endl;
      }

      delete [] index;
      delete [] block;
      delete [] res;

      //if (*(int32_t*)msg.getData() == 0)
      //   break;
   }

   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;

   dlclose(handle);
   UDT::close(u);

   cout << "comp server closed " << ip << " " << port << " " << duration << endl;

   return NULL;
}
