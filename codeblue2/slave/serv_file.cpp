/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 03/18/2009
*****************************************************************************/


#include <slave.h>
#include <iostream>
#include <utime.h>

using namespace std;

void* Slave::fileHandler(void* p)
{
   Slave* self = ((Param2*)p)->serv_instance;
   string filename = self->m_strHomeDir + ((Param2*)p)->filename;
   string sname = ((Param2*)p)->filename;
   int key = ((Param2*)p)->key;
   int mode = ((Param2*)p)->mode;
   int transid = ((Param2*)p)->transid;
   string src_ip = ((Param2*)p)->src_ip;
   int src_port = ((Param2*)p)->src_port;
   string dst_ip = ((Param2*)p)->dst_ip;
   int dst_port = ((Param2*)p)->dst_port;
   unsigned char crypto_key[16];
   unsigned char crypto_iv[8];
   memcpy(crypto_key, ((Param2*)p)->crypto_key, 16);
   memcpy(crypto_iv, ((Param2*)p)->crypto_iv, 8);
   delete (Param2*)p;

   bool bRead = mode & 1;
   bool bWrite = mode & 2;
   bool bSecure = mode & 16;

   bool run = true;

   cout << "rendezvous connect source " << src_ip << " " << src_port << " " << filename << endl;

   if (self->m_DataChn.connect(src_ip, src_port) < 0)
   {
      self->logError(1, src_ip, src_port, sname);
      return NULL;
   }

   if (bSecure)
      self->m_DataChn.setCryptoKey(src_ip, src_port, crypto_key, crypto_iv);

   if (dst_port > 0)
      self->m_DataChn.connect(dst_ip, dst_port);

   //create a new directory or file in case it does not exist
   int change = 0;
   if (mode > 1)
   {
      self->createDir(sname.substr(0, sname.rfind('/')));

      struct stat t;
      if (stat(filename.c_str(), &t) == -1)
      {
         ofstream newfile(filename.c_str());
         newfile.close();
         change = 1;
      }
   }

   cout << "connected\n";

   timeval t1, t2;
   gettimeofday(&t1, 0);
   int64_t rb = 0;
   int64_t wb = 0;

   int32_t cmd = 0;

   while (run)
   {
      if (self->m_DataChn.recv4(src_ip, src_port, transid, cmd) < 0)
         break;

      ifstream ifs;
      ofstream ofs;

      if (5 != cmd)
      {
         int32_t response = -1;

         if (((2 == cmd) || (4 == cmd)) && bWrite)
         {
            ofs.open(filename.c_str(), ios::out | ios::binary | ios::app);
            if (!ofs.fail() && !ofs.bad())
               response = 0;
         }
         else if (((1 == cmd) || (3 == cmd)) && bRead)
         {
            ifs.open(filename.c_str(), ios::in | ios::binary);
            if (!ifs.fail() && !ifs.bad())
               response = 0;
         }

         if (self->m_DataChn.send(src_ip, src_port, transid, (char*)&response, 4) < 0)
            break;

         if (-1 == response)
         {
            ifs.close();
            ofs.close();
            break;
         }
      }

      switch (cmd)
      {
      case 1: // read
         {
            char* param = NULL;
            int tmp = 8 * 2;
            if (self->m_DataChn.recv(src_ip, src_port, transid, param, tmp) < 0)
            {
               run = false;
               break;
            }
            int64_t offset = *(int64_t*)param;
            int64_t size = *(int64_t*)(param + 8);
            delete [] param;

            if (self->m_DataChn.sendfile(src_ip, src_port, transid, ifs, offset, size, bSecure) < 0)
               run = false;

            // update total sent data size
            self->m_SlaveStat.updateIO(src_ip, param[1], (key == 0) ? 1 : 3);

            ifs.close();
            break;
         }

      case 2: // write
         {
            char* param = NULL;
            int tmp = 8 * 2;
            if (self->m_DataChn.recv(src_ip, src_port, transid, param, tmp) < 0)
            {
               run = false;
               break;
            }
            int64_t offset = *(int64_t*)param;
            int64_t size = *(int64_t*)(param + 8);
            delete [] param;

            if (self->m_DataChn.recvfile(src_ip, src_port, transid, ofs, offset, size, bSecure) < 0)
               run = false;
            else
               wb += size;

            // update total received data size
            self->m_SlaveStat.updateIO(src_ip, size, (key == 0) ? 0 : 2);

            if (change != 1)
               change = 2;

            ofs.close();

            if (dst_port > 0)
            {
               self->m_DataChn.send(dst_ip, dst_port, transid, (char*)&cmd, 4);
               int response;
               if ((self->m_DataChn.recv4(dst_ip, dst_port, transid, response) < 0) || (-1 == response))
                  break;

               // replicate data to another node
               char req[16];
               *(int64_t*)req = offset;
               *(int64_t*)(req + 8) = size;

               if (self->m_DataChn.send(dst_ip, dst_port, transid, req, 16) < 0)
                  break;

               ifs.open(filename.c_str());
               self->m_DataChn.sendfile(dst_ip, dst_port, transid, ifs, offset, size);
               ifs.close();
            }

            break;
         }

      case 3: // download
         {
            int64_t offset;
            if (self->m_DataChn.recv8(src_ip, src_port, transid, offset) < 0)
            {
               run = false;
               break;
            }

            ifs.seekg(0, ios::end);
            int64_t size = (int64_t)(ifs.tellg());
            ifs.seekg(0, ios::beg);

            size -= offset;

            int64_t unit = 64000000; //send 64MB each time
            int64_t tosend = size;
            int64_t sent = 0;
            while (tosend > 0)
            {
               int64_t block = (tosend < unit) ? tosend : unit;
               if (self->m_DataChn.sendfile(src_ip, src_port, transid, ifs, offset + sent, block, bSecure) < 0)
               {
                  run = false;
                  break;
               }

               sent += block;
               tosend -= block;
            }

            rb += sent;

            // update total sent data size
            self->m_SlaveStat.updateIO(src_ip, size, (key == 0) ? 1 : 3);

            ifs.close();
            break;
         }

      case 4: // upload
         {
            int64_t offset = 0;
            int64_t size;
            if (self->m_DataChn.recv8(src_ip, src_port, transid, size) < 0)
            {
               run = false;
               break;
            }

            int64_t unit = 64000000; //send 64MB each time
            int64_t torecv = size;
            int64_t recd = 0;

            // previously openned, closed here
            ofs.close();

            while (torecv > 0)
            {
               int64_t block = (torecv < unit) ? torecv : unit;

               ofs.open(filename.c_str(), ios::out | ios::binary | ios::app);
               if (self->m_DataChn.recvfile(src_ip, src_port, transid, ofs, offset + recd, block, bSecure) < 0)
               {
                  run = false;
                  break;
               }
               ofs.close();

               if (dst_port > 0)
               {
                  // write to uplink

                  int write = 2;
                  self->m_DataChn.send(dst_ip, dst_port, transid, (char*)&write, 4);
                  int response;
                  if ((self->m_DataChn.recv4(dst_ip, dst_port, transid, response) < 0) || (-1 == response))
                     break;

                  char req[16];
                  *(int64_t*)req = offset + recd;
                  *(int64_t*)(req + 8) = block;

                  if (self->m_DataChn.send(dst_ip, dst_port, transid, req, 16) < 0)
                     break;

                  ifs.open(filename.c_str(), ios::in | ios::binary);
                  self->m_DataChn.sendfile(dst_ip, dst_port, transid, ifs, offset + recd, block);
                  ifs.close();
               }

               recd += block;
               torecv -= block;
            }

            wb += recd;

            // update total received data size
            self->m_SlaveStat.updateIO(src_ip, size, (key == 0) ? 0 : 2);

            if (change != 1)
               change = 2;

            break;
         }

      case 5: // end session
         if (dst_port > 0)
         {
            // disconnet uplink
            self->m_DataChn.send(dst_ip, dst_port, transid, (char*)&cmd, 4);
            self->m_DataChn.recv4(dst_ip, dst_port, transid, cmd);
         }

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

   cout << "file server closed " << src_ip << " " << src_port << " " << avgRS << endl;

   char* tmp = new char[64 + sname.length()];
   sprintf(tmp, "file server closed ... %s %f %f.", sname.c_str(), avgRS, avgWS);
   self->m_SectorLog.insert(tmp);
   delete [] tmp;

   //report to master the task is completed
   self->report(transid, sname, change);

   self->m_DataChn.send(src_ip, src_port, transid, (char*)&cmd, 4);
   //self->m_DataChn.remove(src_ip, src_port);

   return NULL;
}

void* Slave::copy(void* p)
{
   Slave* self = ((Param3*)p)->serv_instance;
   int transid = ((Param3*)p)->transid;
   time_t ts = ((Param3*)p)->timestamp;
   string src = ((Param3*)p)->src;
   string dst = ((Param3*)p)->dst;
   delete (Param3*)p;

   SectorMsg msg;
   msg.setType(110); // open the file
   msg.setKey(0);

   int32_t mode = 1;
   msg.setData(0, (char*)&mode, 4);
   int32_t localport = self->m_DataChn.getPort();
   msg.setData(4, (char*)&localport, 4);
   msg.setData(8, src.c_str(), src.length() + 1);

   if (self->m_GMP.rpc(self->m_strMasterIP.c_str(), self->m_iMasterPort, &msg, &msg) < 0)
      return NULL;
   if (msg.getType() < 0)
      return NULL;

   string ip = msg.getData();
   int port = *(int*)(msg.getData() + 64);
   int session = *(int*)(msg.getData() + 68);

   int64_t size = *(int64_t*)(msg.getData() + 72);

   //cout << "rendezvous connect " << ip << " " << port << endl;

   if (self->m_DataChn.connect(ip, port) < 0)
      return NULL;

   // download command: 3
   int32_t cmd = 3;
   self->m_DataChn.send(ip, port, session, (char*)&cmd, 4);

   int response = -1;
   if ((self->m_DataChn.recv4(ip, port, session, response) < 0) || (-1 == response))
      return NULL;

   int64_t offset = 0;
   if (self->m_DataChn.send(ip, port, session, (char*)&offset, 8) < 0)
      return NULL;

   //copy to .tmp first, then move to real location
   self->createDir(string(".tmp") + dst.substr(0, dst.rfind('/')));

   ofstream ofs;
   ofs.open((self->m_strHomeDir + ".tmp" + dst).c_str(), ios::out | ios::binary | ios::trunc);

   int64_t unit = 64000000; //send 64MB each time
   int64_t torecv = size;
   int64_t recd = 0;
   while (torecv > 0)
   {
      int64_t block = (torecv < unit) ? torecv : unit;
      if (self->m_DataChn.recvfile(ip, port, session, ofs, offset + recd, block) < 0)
         unlink((self->m_strHomeDir + ".tmp" + dst).c_str());

      recd += block;
      torecv -= block;
   }

   ofs.close();

   // update total received data size
   self->m_SlaveStat.updateIO(ip, size, 0);

   cmd = 5;
   self->m_DataChn.send(ip, port, session, (char*)&cmd, 4);
   self->m_DataChn.recv4(ip, port, session, cmd);

   //utime: update timestamp according to the original copy
   utimbuf ut;
   ut.actime = ts;
   ut.modtime = ts;
   utime((self->m_strHomeDir + ".tmp" + dst).c_str(), &ut);

   self->createDir(dst.substr(0, dst.rfind('/')));
   string rhome = self->reviseSysCmdPath(self->m_strHomeDir);
   string rfile = self->reviseSysCmdPath(dst);
   system(("mv " + rhome + ".tmp" + rfile + " " + rhome + rfile).c_str());

   // if the file has been modified during the replication, remove this replica
   int type = (src == dst) ? 3 : 1;
   if (self->report(transid, dst, type) < 0)
      system(("rm " + rhome + rfile).c_str());

   return NULL;
}
