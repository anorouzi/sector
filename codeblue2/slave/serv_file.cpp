/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 12/06/2008
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
   Transport* datachn = ((Param2*)p)->datachn;
   int dataport = ((Param2*)p)->dataport;
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

   int32_t cmd;
   bool run = true;

   cout << "rendezvous connect source " << src_ip << " " << src_port << " " << filename << endl;

   if (datachn->connect(src_ip.c_str(), src_port) < 0)
   {
      self->logError(1, src_ip, src_port, sname);
      return NULL;
   }

   if (bSecure)
      datachn->initCoder(crypto_key, crypto_iv);


   Transport uplink;
   if (dst_port > 0)
   {
      uplink.open(dataport, true, true);
      uplink.connect(dst_ip.c_str(), dst_port);
   }

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

   int32_t response = 0;

   while (run)
   {
      if (datachn->recv((char*)&cmd, 4) < 0)
         break;

      ifstream ifs;
      ofstream ofs;

      if (5 != cmd)
      {
         response = -1;

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

         if (datachn->send((char*)&response, 4) < 0)
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
            int64_t param[2];
            if (datachn->recv((char*)param, 8 * 2) < 0)
            {
               run = false;
               break;
            }

            if (datachn->sendfileEx(ifs, param[0], param[1], bSecure) < 0)
               run = false;

            // update total sent data size
            self->m_SlaveStat.updateIO(src_ip, param[1], (key == 0) ? 1 : 3);

            ifs.close();
            break;
         }

      case 2: // write
         {
            int64_t param[2];

            if (datachn->recv((char*)param, 8 * 2) < 0)
            {
               run = false;
               break;
            }

            if (datachn->recvfileEx(ofs, param[0], param[1], bSecure) < 0)
               run = false;
            else
               wb += param[1];

            // update total received data size
            self->m_SlaveStat.updateIO(src_ip, param[1], (key == 0) ? 0 : 2);

            if (change != 1)
               change = 2;

            ofs.close();

            if (dst_port > 0)
            {
               // replicate data to another node
               char req[20];
               *(int32_t*)req = 2; // cmd write
               *(int64_t*)(req + 4) = param[0];
               *(int64_t*)(req + 12) = param[1];

               int32_t response = -1;

               if (uplink.send(req, 20) < 0)
                  break;
               if ((uplink.recv((char*)&response, 4) < 0) || (-1 == response))
                  break;

               ifs.open(filename.c_str());
               uplink.sendfile(ifs, param[0], param[1]);
               ifs.close();
            }

            break;
         }

      case 3: // download
         {
            int64_t offset = 0;
            int64_t size = 0;

            if (datachn->recv((char*)&offset, 8) < 0)
            {
               run = false;
               break;
            }

            ifs.seekg(0, ios::end);
            size = (int64_t)(ifs.tellg());
            ifs.seekg(0, ios::beg);

            size -= offset;

            if (datachn->send((char*)&size, 8) < 0)
            {
               run = false;
               ifs.close();
               break;
            }

            if (datachn->sendfileEx(ifs, offset, size, bSecure) < 0)
               run = false;
            else
               rb += size;

            // update total sent data size
            self->m_SlaveStat.updateIO(src_ip, size, (key == 0) ? 1 : 3);

            break;
         }

      case 4: // upload
         {
            int64_t offset = 0;
            int64_t size = 0;

            if (datachn->recv((char*)&size, 8) < 0)
            {
               run = false;
               break;
            }

            if (datachn->recvfileEx(ofs, offset, size, bSecure) < 0)
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
                // upload
               char req[12];
               *(int32_t*)req = 4; // cmd write
               *(int64_t*)(req + 4) = size;

               int32_t response = -1;

               if (uplink.send(req, 12) < 0)
                  break;
               if ((uplink.recv((char*)&response, 4) < 0) || (-1 == response))
                  break;

               ifs.open(filename.c_str());
               uplink.sendfile(ifs, 0, size);
               ifs.close();
            }

            break;
         }

      case 5: // end session
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

   if (dst_port > 0)
   {
      cmd = 5;
      uplink.send((char*)&cmd, 4);
      uplink.recv((char*)&cmd, 4);
      uplink.close();
   }

   datachn->send((char*)&cmd, 4);
   if (bSecure)
      datachn->releaseCoder();
   datachn->close();
   delete datachn;

   return NULL;
}

void* Slave::copy(void* p)
{
   Slave* self = ((Param3*)p)->serv_instance;
   time_t ts = ((Param3*)p)->timestamp;
   string src = ((Param3*)p)->src;
   string dst = ((Param3*)p)->dst;
   delete (Param3*)p;

   SectorMsg msg;
   msg.setType(110); // open the file
   msg.setKey(0);

   Transport datachn;
   int port = 0;
   datachn.open(port, true, true);

   int mode = 1;
   msg.setData(0, (char*)&port, 4);
   msg.setData(4, (char*)&mode, 4);
   msg.setData(8, src.c_str(), src.length() + 1);

   if (self->m_GMP.rpc(self->m_strMasterIP.c_str(), self->m_iMasterPort, &msg, &msg) < 0)
      return NULL;
   if (msg.getType() < 0)
      return NULL;

   cout << "rendezvous connect " << msg.getData() << " " << *(int*)(msg.getData() + 68) << endl;

   if (datachn.connect(msg.getData(), *(int*)(msg.getData() + 68)) < 0)
      return NULL;

   int32_t cmd = 3;
   int64_t offset = 0LL;
   int64_t size;
   int32_t response = -1;

   char req[12];
   *(int32_t*)req = cmd;
   *(int64_t*)(req + 4) = offset;

   if (datachn.send(req, 12) < 0)
      return NULL;
   if ((datachn.recv((char*)&response, 4) < 0) || (-1 == response))
      return NULL;
   if (datachn.recv((char*)&size, 8) < 0)
      return NULL;

   //copy to .tmp first, then move to real location
   self->createDir(string(".tmp") + dst.substr(0, dst.rfind('/')));

   ofstream ofs;
   ofs.open((self->m_strHomeDir + ".tmp" + dst).c_str(), ios::out | ios::binary | ios::trunc);
   if (datachn.recvfile(ofs, offset, size) < 0)
      unlink((self->m_strHomeDir + ".tmp" + dst).c_str());

   ofs.close();

   // update total received data size
   self->m_SlaveStat.updateIO(msg.getData(), size, 0);

   cmd = 5;
   datachn.send((char*)&cmd, 4);
   datachn.recv((char*)&cmd, 4);
   datachn.close();

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
   if (self->report(0, dst, type) < 0)
      system(("rm " + rhome + rfile).c_str());

   return NULL;
}
