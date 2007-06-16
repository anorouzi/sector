/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 06/07/2007
*****************************************************************************/

#include <server.h>
#include <util.h>
#include <dlfcn.h>
#include <fsclient.h>

using namespace cb;

void* Server::SPEHandler(void* p)
{
   Server* self = ((Param4*)p)->serv_instance;
   Transport* datachn = ((Param4*)p)->datachn;
   string ip = ((Param4*)p)->client_ip;
   int ctrlport = ((Param4*)p)->client_ctrl_port;
   int dataport = ((Param4*)p)->client_data_port;
   int speid = ((Param4*)p)->speid;
   string function = ((Param4*)p)->function;
   char* param = ((Param4*)p)->param;
   int psize = ((Param4*)p)->psize;
   delete (Param4*)p;
   CCBMsg msg;

   cout << "rendezvous connect " << ip << " " << dataport << endl;
   if (datachn->connect(ip.c_str(), dataport) < 0)
      return NULL;

   cout << "locating so " << (self->m_strHomeDir + function + ".so") << endl;
   void* handle = dlopen((self->m_strHomeDir + function + ".so").c_str(), RTLD_LAZY);
   if (NULL == handle)
      return NULL;

   cout << "so found " << "locating process " << function << endl;

   int (*process)(const char*, const int&, char*, int&, const char*, const int&);
   process = (int (*) (const char*, const int&, char*, int&, const char*, const int&) )dlsym(handle, function.c_str());
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
      char dataseg[80];
      if (datachn->recv(dataseg, 80) < 0)
         break;

      datafile = dataseg;
      offset = *(int64_t*)(dataseg + 64);
      rows = *(int64_t*)(dataseg + 72);
      index = new int64_t[rows + 1];

      cout << "new job " << datafile << " " << offset << " " << rows << endl;

      // read data
      if (self->m_LocalFile.lookup(datafile.c_str(), NULL) > 0)
      {
         ifstream idx;
         idx.open((self->m_strHomeDir + datafile + ".idx").c_str());
         idx.seekg(offset * 8);
         idx.read((char*)index, (rows + 1) * 8);
         idx.close();

         size = index[rows] - index[0];
         cout << "to read data " << size << endl;
         block = new char[size];

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
         if (f->open(datafile.c_str()) < 0)
         {
            delete [] index;
            return NULL;
         }
         if (f->readridx((char*)index, offset, rows) < 0)
         {
            delete [] index;
            return NULL;
         }

         size = index[rows] - index[0];
         block = new char[size];

         if (f->read(block, index[0], size) < 0)
         {
            delete [] index;
            delete [] block;
            return NULL;
         }

         f->close();
         Client::releaseFileHandle(f);
      }

      res = new char[size];
      rsize = 0;
      gettimeofday(&t3, 0);
      for (int i = 0; i < rows; ++ i)
      {
         //cout << "to process " << index[i] - index[0] << " " << index[i + 1] - index[i] << endl;
         process(block + index[i] - index[0], index[i + 1] - index[i], res + rsize, rs, param, psize);
         rsize += rs;
         rs = size - rsize;

         gettimeofday(&t4, 0);
         if (t4.tv_sec - t3.tv_sec > 1)
         {
            progress = i * 100 / rows;
            msg.setData(4, (char*)&progress, 4);
            msg.m_iDataLength = 4 + 8;
            if (self->m_GMP.rpc(ip.c_str(), ctrlport, &msg, &msg) < 0)
               return NULL;
            t3 = t4;
         }
      }

      progress = 100;
      msg.setData(4, (char*)&progress, 4);
      msg.setData(8, (char*)&rsize, 4);
      msg.m_iDataLength = 4 + 12;
      if (self->m_GMP.rpc(ip.c_str(), ctrlport, &msg, &msg) < 0)
         return NULL;

      cout << "sending data back... " << rsize << " " << *(int*)res << endl;

      datachn->send(res, rsize);

      delete [] index;
      delete [] block;
      delete [] res;
      index = NULL;
      block = NULL;
      res = NULL;
   }

   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;

   dlclose(handle);
   datachn->close();
   delete datachn;

   cout << "comp server closed " << ip << " " << ctrlport << " " << duration << endl;

   return NULL;
}
