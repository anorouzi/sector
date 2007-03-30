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

using namespace cb;

void* Server::SPEHandler(void* p)
{
   Server* self = ((Param4*)p)->s;
   UDTSOCKET u = ((Param4*)p)->u;
   char* ip = ((Param4*)p)->ip;
   int port = ((Param4*)p)->port;
   SPE spe = ((Param4*)p)->spe;
   delete (Param4*)p;
   CCBMsg msg;

   UDTSOCKET lu = u;

   u = UDT::accept(u, NULL, NULL);
   UDT::close(lu);

   timeval t1, t2;
   gettimeofday(&t1, 0);


   int size = spe.m_llSize;
   char* block = new char[size];
   

   //check if file already exists!
   if (self->m_LocalFile.lookup(spe.m_strDataFile.c_str(), NULL) > 0)
   {
      ifstream ifs;
      ifs.open((self->m_strHomeDir + spe.m_strDataFile).c_str());
      ifs.read(block, size);
      ifs.close();

cout << "read data into block...\n";

   }
   else
   {
      int fid = DHash::hash(spe.m_strDataFile.c_str(), m_iKeySpace);
      Node n;
      if (- 1 == self->m_Router.lookup(fid, &n))
         return NULL;

      msg.setType(1); // locate file
      msg.setData(0, spe.m_strDataFile.c_str(), spe.m_strDataFile.length() + 1);
      msg.m_iDataLength = 4 + spe.m_strDataFile.length() + 1;

      if (self->m_GMP.rpc(n.m_pcIP, n.m_iAppPort, &msg, &msg) < 0)
         return NULL;

      string srcip = msg.getData();
      int srcport = *(int32_t*)(msg.getData() + 64);

      int protocol = 1; // UDT
      int mode = 1; // READ ONLY

      msg.setType(2); // open the file
      msg.setData(0, spe.m_strDataFile.c_str(), spe.m_strDataFile.length() + 1);
      msg.setData(64, (char*)&protocol, 4);
      msg.setData(68, (char*)&mode, 4);
      msg.m_iDataLength = 4 + 64 + 4 + 4;

      if (self->m_GMP.rpc(srcip.c_str(), srcport, &msg, &msg) < 0)
         return NULL;

      msg.setType(-8);
      msg.m_iDataLength = 4;

      UDTSOCKET u = UDT::socket(AF_INET, SOCK_STREAM, 0);

      sockaddr_in serv_addr;
      serv_addr.sin_family = AF_INET;
      serv_addr.sin_port = *(int*)(msg.getData()); // port
      inet_pton(AF_INET, srcip.c_str(), &serv_addr.sin_addr);
      memset(&(serv_addr.sin_zero), '\0', 8);

      if (UDT::ERROR == UDT::connect(u, (sockaddr*)&serv_addr, sizeof(serv_addr)))
         return NULL;

      int h;
      if (UDT::ERROR == UDT::recv(u, block, size, 0, &h))
         return NULL;
   }


cout << "locating so " << (self->m_strHomeDir + spe.m_strOperator + ".so") << endl;

   void* handle = dlopen((self->m_strHomeDir + spe.m_strOperator + ".so").c_str(), RTLD_LAZY);
   if (NULL == handle)
      return NULL;

cout << "so found " << "locating process " << spe.m_strOperator << endl;

   int (*process)(const char*, const int&, char*, int&, const char*, const int&);
   process = (int (*) (const char*, const int&, char*, int&, const char*, const int&) )dlsym(handle, spe.m_strOperator.c_str());
   if (NULL == process)
   {
      cout << dlerror() <<  endl;
      return NULL;
   }

cout << "process found~\n";

   char* res = new char[size];
   int rsize = 0;
   int rs = size;
   for (int progress = 0; progress < size; progress += spe.m_iUnitSize)
   {
      process(block + progress, spe.m_iUnitSize, res + rsize, rs, spe.m_pcParam, spe.m_iParamSize);
      rsize += rs;
      rs = size - rsize;
   }

   dlclose(handle);

   msg.setType(1); // success, return result
   msg.setData(0, (char*)&(spe.m_uiID), 4);
   msg.setData(4, (char*)&rsize, 4);
   msg.m_iDataLength = 4 + 8;
   if (self->m_GMP.rpc(ip, port, &msg, &msg) < 0)
      return NULL;

cout << "sending data back... " << rsize << endl;

   int h;
   UDT::send(u, res, size, 0, &h);


   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;

   UDT::close(u);

   cout << "comp server closed " << ip << " " << port << " " << duration << endl;

   delete [] block;
   delete [] res;

   delete [] ip;
   if (NULL != spe.m_pcParam)
      delete [] spe.m_pcParam;

   return NULL;
}
