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
   int t = ((Param2*)p)->t;
   int conn = ((Param2*)p)->c;
   int mode = ((Param2*)p)->m;
   delete (Param2*)p;

   int32_t cmd;
   bool run = true;

/*
   // timed wait on accept!
   if (1 == conn)
   {
      timeval tv;
      UDT::UDSET readfds;

      tv.tv_sec = 60;
      tv.tv_usec = 0;

      UD_ZERO(&readfds);
      UD_SET(u, &readfds);

      int res = UDT::select(0, &readfds, NULL, NULL, &tv);

      if (UDT::ERROR == res)
         return NULL;
   }
   else
   {
      timeval tv;
      fd_set readfds;

      tv.tv_sec = 60;
      tv.tv_usec = 0;

      FD_ZERO(&readfds);
      FD_SET(t, &readfds);

      select(t+1, &readfds, NULL, NULL, &tv);

      if (!FD_ISSET(t, &readfds))
         return NULL;
   }
*/

   UDTSOCKET lu = u;
   int lt = t;

   if (1 == conn)
   {
      u = UDT::accept(u, NULL, NULL);
      UDT::close(lu);
   }
   else
   {
      t = accept(t, NULL, NULL);
      ::close(lt);
   }

//   self->m_KBase.m_iNumConn ++;

   filename = self->m_strHomeDir + filename;

   timeval t1, t2;
   gettimeofday(&t1, 0);

   int64_t rb = 0;
   int64_t wb = 0;

   int32_t response = 0;

   while (run)
   {
      if (1 == conn)
      {
         if (UDT::recv(u, (char*)&cmd, 4, 0) < 0)
            continue;
      }
      else
      {
         if (::recv(t, (char*)&cmd, 4, 0) <= 0)
            continue;
      }

      if (4 != cmd)
      {
         if ((2 == cmd) || (5 == cmd))
         {
            if (0 == mode)
               response = -1;
         }
         else
            response = 0;

         if (1 == conn)
         {
            if (UDT::send(u, (char*)&response, 4, 0) < 0)
               continue;
         }
         else
         {
            if (::send(t, (char*)&response, 4, 0) < 0)
               continue;
         }

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

            if (1 == conn)
            {
               if (UDT::recv(u, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               if (UDT::sendfile(u, ifs, param[0], param[1]) < 0)
                  run = false;
               else
                  rb += param[1];
            }
            else
            {
               if (::recv(t, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               ifs.seekg(param[0]);

               int unit = 10240000;
               char* data = new char[unit];
               int ssize = 0;

               while (run && (ssize + unit <= param[1]))
               {
                  ifs.read(data, unit);

                  int ts = 0;
                  while (ts < unit)
                  {
                     int ss = ::send(t, data + ts, unit - ts, 0);
                     if (ss < 0)
                     {
                        run = false;
                        break;
                     }

                     ts += ss;
                  }

                  ssize += unit;
               }

               if (ssize < param[1])
               {
                  ifs.read(data, param[1] - ssize);

                  int ts = 0;
                  while (ts < unit)
                  {
                     int ss = ::send(t, data + ssize, param[1] - ssize, 0);
                     if (ss < 0)
                     {
                        run = false;
                        break;
                     }

                     ts += ss;
                  }

               }

               if (run)
                  rb += param[1];

               delete [] data;
            }

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

            if (1 == conn)
            {
               if (UDT::recv(u, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               ofstream ofs;
               ofs.open(filename.c_str(), ios::out | ios::binary | ios::app);

               if (UDT::recvfile(u, ofs, param[0], param[1]) < 0)
                  run = false;
               else
                  wb += param[1];

               ofs.close();
            }
            else
            {
               if (::recv(t, (char*)param, 8 * 2, 0) < 0)
                  run = false;

               char* temp = new char[param[1]];
               int rs = 0;
               while (rs < param[1])
               {
                  int r = ::recv(t, temp + rs, param[1] - rs, 0);
                  if (r < 0)
                  {
                     run = false;
                     break;
                  }

                  rs += r;
               }

               ofstream ofs;
               ofs.open(filename.c_str(), ios::out | ios::binary | ios::app);
               ofs.seekp(param[0], ios::beg);
               ofs.write(temp, param[1]);
               ofs.close();

               delete [] temp;
               wb += param[1];
            }

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

            if (1 == conn)
            {
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
            }
            else
            {
               if (::recv(t, (char*)&offset, 8, 0) < 0)
               {
                  run = false;
                  break;
               }

               size -= offset;

               if (::send(t, (char*)&size, 8, 0) < 0)
                  run = false;

               int unit = 10240000;
               char* data = new char[unit];
               int ssize = 0;

               while (run && (ssize + unit <= size))
               {
                  ifs.read(data, unit);

                  int ts = 0;
                  while (ts < unit)
                  {
                     int ss = ::send(t, data + ts, unit - ts, 0);
                     if (ss < 0)
                     {
                        run = false;
                        break;
                     }

                     ts += ss;
                  }

                  ssize += unit;
               }

               if (ssize < size)
               {
                  ifs.read(data, size - ssize);

                  int ts = 0;
                  while (ts < size - ssize)
                  {
                     int ss = ::send(t, data + ssize, size - ssize, 0);
                     if (ss < 0)
                     {
                        run = false;
                        break;
                     }

                     ts += ss;
                  }

               }

               delete [] data;

               if (run)
                  rb += size;
            }

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

            if (1 == conn)
            {
               //if (UDT::recv(u, (char*)&offset, 8, 0) < 0)
               //{
               //   run = false;
               //   break;
               //}
               //offset = 0;

               if (UDT::recv(u, (char*)&size, 8, 0) < 0)
               {
                  run = false;
                  break;
               }

               if (UDT::recvfile(u, ofs, offset, size) < 0)
                  run = false;
               else
                  wb += size;
            }
            else
            {
               if (::recv(t, (char*)&size, 8, 0) < 0)
               {
                  run = false;
                  break;
               }

               const int unit = 1024000;
               char* data = new char [unit];
               int64_t rsize = 0;

               while (rsize < size)
               {
                  int rs = ::recv(t, data, (unit < size - rsize) ? unit : size - rsize, 0);

                  if (rs < 0)
                  {
                     run = false;
                     break;
                  }

                  ofs.write(data, rs);

                  rsize += rs;
               }

               delete [] data;

               if (run)
                  rb += size;
            }

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

   sockaddr_in addr;
   int addrlen = sizeof(addr);
   if (1 == conn)
      UDT::getpeername(u, (sockaddr*)&addr, &addrlen);
   else
      getpeername(t, (sockaddr*)&addr, (socklen_t*)&addrlen);
   char ip[64];
   inet_ntop(AF_INET, &(addr.sin_addr), ip, 64);
   int port = ntohs(addr.sin_port);
   
   self->m_PerfLog.insert(ip, port, filename.c_str(), duration, avgRS, avgWS);

   if (1 == conn)
      UDT::close(u);
   else
      close(t);

//   self->m_KBase.m_iNumConn --;

   cout << "file server closed " << ip << " " << port << " " << avgRS << endl;

   return NULL;
}


