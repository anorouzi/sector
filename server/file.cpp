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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/sendfile.h>
#include <server.h>
#include <assert.h>
#include <sstream>
#include <signal.h>
#include <util.h>

using namespace cb;

void* Server::fileHandler(void* p)
{
   Server* self = ((Param2*)p)->s;
   string filename = ((Param2*)p)->fn;
   UDTSOCKET u = ((Param2*)p)->u;
   string ip = ((Param2*)p)->ip;
   int port = ((Param2*)p)->p;
   int mode = ((Param2*)p)->m;
   delete (Param2*)p;

   int32_t cmd;
   bool run = true;

   sockaddr_in cli_addr;
   cli_addr.sin_family = AF_INET;
   cli_addr.sin_port = port;
   inet_pton(AF_INET, ip.c_str(), &cli_addr.sin_addr);
   memset(&(cli_addr.sin_zero), '\0', 8);

   cout << "rendezvous connect " << ip << " " << port << endl;

   if (UDT::ERROR == UDT::connect(u, (sockaddr*)&cli_addr, sizeof(sockaddr_in)))
      return NULL;

//   self->m_KBase.m_iNumConn ++;

   filename = self->m_strHomeDir + filename;

   timeval t1, t2;
   gettimeofday(&t1, 0);

   int64_t rb = 0;
   int64_t wb = 0;

   int32_t response = 0;

   while (run)
   {
      if (UDT::recv(u, (char*)&cmd, 4, 0) < 0)
         continue;

      if (4 != cmd)
      {
         if ((2 == cmd) || (5 == cmd))
         {
            if (0 == mode)
               response = -1;
         }
         else
            response = 0;

         if (UDT::send(u, (char*)&response, 4, 0) < 0)
            continue;

         if (-1 == response)
            continue;
      }

      switch (cmd)
      {
      case 1:
         {
            if (0 < (mode & 1))
               response = 0;
            else
               response = -1;

            // READ LOCK

            int64_t param[2];

            ifstream ifs(filename.c_str(), ios::in | ios::binary);

            if (UDT::recv(u, (char*)param, 8 * 2, 0) < 0)
               run = false;

            if (UDT::sendfile(u, ifs, param[0], param[1]) < 0)
               run = false;

            ifs.close();

            // UNLOCK

            break;
         }

      case 2:
         {
            if (0 < (mode & 2))
               response = 0;
            else
               response = -1;

            // WRITE LOCK

            int64_t param[2];

            if (UDT::recv(u, (char*)param, 8 * 2, 0) < 0)
               run = false;

            ofstream ofs;
            ofs.open(filename.c_str(), ios::out | ios::binary | ios::app);

            if (UDT::recvfile(u, ofs, param[0], param[1]) < 0)
               run = false;
            else
               wb += param[1];

            ofs.close();

            // UNLOCK

            break;
         }

      case 3:
         {
            if (0 < (mode & 1))
               response = 0;
            else
               response = -1;

            // READ LOCK

            int64_t offset = 0;
            int64_t size = 0;

            ifstream ifs(filename.c_str(), ios::in | ios::binary);
            ifs.seekg(0, ios::end);
            size = (int64_t)(ifs.tellg());
            ifs.seekg(0, ios::beg);

            if (UDT::recv(u, (char*)&offset, 8, 0) < 0)
            {
               run = false;
               break;
            }

            size -= offset;

            if (UDT::send(u, (char*)&size, 8, 0) < 0)
            {
               run = false;
               ifs.close();
               break;
            }

            if (UDT::sendfile(u, ifs, offset, size) < 0)
               run = false;
            else
               rb += size;

            ifs.close();

            // UNLOCK

            break;
         }

      case 5:
         {
            if (0 < (mode & 1))
               response = 0;
            else
               response = -1;

            // WRITE LOCK

            int64_t offset = 0;
            int64_t size = 0;

            ofstream ofs(filename.c_str(), ios::out | ios::binary | ios::trunc);

            if (UDT::recv(u, (char*)&size, 8, 0) < 0)
            {
               run = false;
               break;
            }

            if (UDT::recvfile(u, ofs, offset, size) < 0)
               run = false;
            else
               wb += size;

            ofs.close();

            // UNLOCK
            break;
         }

      case 4:
         run = false;
         break;

      default:
         break;
      }
   }

   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;
   double avgRS = 0;
   double avgWS = 0;
   if (duration > 0)
   {
      avgRS = rb / duration * 8.0 / 1000000.0;
      avgWS = wb / duration * 8.0 / 1000000.0;
   }

   self->m_PerfLog.insert(ip.c_str(), port, filename.c_str(), duration, avgRS, avgWS);

   UDT::close(u);

//   self->m_KBase.m_iNumConn --;

   cout << "file server closed " << ip << " " << port << " " << avgRS << endl;

   return NULL;
}


