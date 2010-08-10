/*****************************************************************************
Copyright (c) 2005 - 2010, The Board of Trustees of the University of Illinois.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 07/28/2010
*****************************************************************************/


#include <writelog.h>
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
   string client_ip = ((Param2*)p)->client_ip;
   int client_port = ((Param2*)p)->client_port;
   unsigned char crypto_key[16];
   unsigned char crypto_iv[8];
   memcpy(crypto_key, ((Param2*)p)->crypto_key, 16);
   memcpy(crypto_iv, ((Param2*)p)->crypto_iv, 8);
   string master_ip = ((Param2*)p)->master_ip;
   int master_port = ((Param2*)p)->master_port;
   delete (Param2*)p;

   // uplink and downlink addresses for write, no need for read
   string src_ip;
   int src_port = -1;
   string dst_ip;
   int dst_port = -1;

   // IO permissions
   bool bRead = mode & 1;
   bool bWrite = mode & 2;
   bool bSecure = mode & 16;

   bool run = true;

   self->m_SectorLog << LogStringTag(LogTag::START, LogLevel::SCREEN) << "rendezvous connect source " << client_ip << " " << client_port << " " << filename << LogStringTag(LogTag::END);

   if (self->m_DataChn.connect(client_ip, client_port) < 0)
   {
      self->m_SectorLog << LogStringTag(LogTag::START, LogLevel::LEVEL_3) << "failed to connect to file client " << client_ip << " " << client_port << " " << filename << LogStringTag(LogTag::END);
      return NULL;
   }

   self->m_SectorLog << LogStringTag(LogTag::START, LogLevel::SCREEN) << "connected." <<  LogStringTag(LogTag::END);

   if (bSecure)
      self->m_DataChn.setCryptoKey(client_ip, client_port, crypto_key, crypto_iv);

   //create a new directory or file in case it does not exist
   int change = FileChangeType::FILE_UPDATE_NO;
   if (mode > 1)
   {
      self->createDir(sname.substr(0, sname.rfind('/')));

      struct stat64 t;
      if (stat64(filename.c_str(), &t) == -1)
      {
         ofstream newfile(filename.c_str(), ios::out | ios::binary | ios::trunc);
         newfile.close();
         change = FileChangeType::FILE_UPDATE_WRITE;
      }
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);
   int64_t rb = 0;
   int64_t wb = 0;

   int32_t cmd = 0;

   WriteLog writelog;

   fstream fhandle;
   fhandle.open(filename.c_str(), ios::in | ios::out | ios::binary);

   while (!fhandle.fail() && run && self->m_bDiskHealth && self->m_bNetworkHealth)
   {
      if (self->m_DataChn.recv4(client_ip, client_port, transid, cmd) < 0)
         break;

      switch (cmd)
      {
      case 1: // read
         {
            char* param = NULL;
            int tmp = 8 * 2;
            if (self->m_DataChn.recv(client_ip, client_port, transid, param, tmp) < 0)
            {
               run = false;
               break;
            }
            int64_t offset = *(int64_t*)param;
            int64_t size = *(int64_t*)(param + 8);
            delete [] param;

            int32_t response = bRead ? 0 : -1;
            if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
               break;
            if (response == -1)
               break;

            if (self->m_DataChn.sendfile(client_ip, client_port, transid, fhandle, offset, size, bSecure) < 0)
               run = false;
            else
               rb += size;

            // update total sent data size
            self->m_SlaveStat.updateIO(client_ip, param[1], (key == 0) ? +SlaveStat::SYS_OUT : +SlaveStat::CLI_OUT);

            break;
         }

      case 2: // write
         {
            if (!bWrite)
            {
               // if the client does not have write permission, disconnect it immediately
               run = false;
               break;
            }

            //receive offset and size information from uplink
            char* param = NULL;
            int tmp = 8 * 2;
            if (self->m_DataChn.recv(src_ip, src_port, transid, param, tmp) < 0)
               break;

            int64_t offset = *(int64_t*)param;
            int64_t size = *(int64_t*)(param + 8);
            delete [] param;

            // no secure transfer between two slaves
            bool secure_transfer = bSecure;
            if ((client_ip != src_ip) || (client_port != src_port))
               secure_transfer = false;

            bool io_status = (size > 0); 
            if (!io_status || (self->m_DataChn.recvfile(src_ip, src_port, transid, fhandle, offset, size, secure_transfer) < size))
               io_status = false;

            //TODO: send imcomplete write to next slave on chain, rather than -1

            if (dst_port > 0)
            {
               // send offset and size parameters
               char req[16];
               *(int64_t*)req = offset;
               if (io_status)
                   *(int64_t*)(req + 8) = size;
               else
                   *(int64_t*)(req + 8) = -1;
               self->m_DataChn.send(dst_ip, dst_port, transid, req, 16);

               // send the data to the next replica in the chain
               if (size > 0)
                  self->m_DataChn.sendfile(dst_ip, dst_port, transid, fhandle, offset, size);
            }

            if (!io_status)
               break;

            wb += size;

            // update total received data size
            self->m_SlaveStat.updateIO(src_ip, size, (key == 0) ? +SlaveStat::SYS_IN : +SlaveStat::CLI_IN);

            // update write log
            writelog.insert(offset, size);

            // file has been changed
            change = FileChangeType::FILE_UPDATE_WRITE;

            break;
         }

      case 3: // download
         {
            int64_t offset;
            if (self->m_DataChn.recv8(client_ip, client_port, transid, offset) < 0)
            {
               run = false;
               break;
            }

            int32_t response = bRead ? 0 : -1;
            if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
               break;
            if (response == -1)
               break;

            fhandle.seekg(0, ios::end);
            int64_t size = (int64_t)(fhandle.tellg());
            fhandle.seekg(0, ios::beg);

            size -= offset;

            int64_t unit = 64000000; //send 64MB each time
            int64_t tosend = size;
            int64_t sent = 0;
            while (tosend > 0)
            {
               int64_t block = (tosend < unit) ? tosend : unit;
               if (self->m_DataChn.sendfile(client_ip, client_port, transid, fhandle, offset + sent, block, bSecure) < 0)
               {
                  run = false;
                  break;
               }

               sent += block;
               tosend -= block;
            }

            rb += sent;

            // update total sent data size
            self->m_SlaveStat.updateIO(client_ip, size, (key == 0) ? +SlaveStat::SYS_OUT : +SlaveStat::CLI_OUT);

            break;
         }

      case 4: // upload
         {
            if (!bWrite)
            {
               // if the client does not have write permission, disconnect it immediately
               run = false;
               break;
            }

            int64_t offset = 0;
            int64_t size;
            if (self->m_DataChn.recv8(client_ip, client_port, transid, size) < 0)
            {
               run = false;
               break;
            }

            //TODO: check available size
            int32_t response = 1;
            if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
               break;
            if (response == -1)
               break;

            int64_t unit = 64000000; //send 64MB each time
            int64_t torecv = size;
            int64_t recd = 0;

            // no secure transfer between two slaves
            bool secure_transfer = bSecure;
            if ((client_ip != src_ip) || (client_port != src_port))
               secure_transfer = false;

            while (torecv > 0)
            {
               int64_t block = (torecv < unit) ? torecv : unit;

               if (self->m_DataChn.recvfile(src_ip, src_port, transid, fhandle, offset + recd, block, secure_transfer) < 0)
               {
                  run = false;
                  break;
               }

               if (dst_port > 0)
               {
                  // write to uplink for next replica in the chain
                  if (self->m_DataChn.sendfile(dst_ip, dst_port, transid, fhandle, offset + recd, block) < 0)
                     break;
               }

               recd += block;
               torecv -= block;
            }

            wb += recd;

            // update total received data size
            self->m_SlaveStat.updateIO(src_ip, size, (key == 0) ? +SlaveStat::SYS_IN : +SlaveStat::CLI_IN);

            // update write log
            writelog.insert(0, size);

            // file has been changed
            change = FileChangeType::FILE_UPDATE_WRITE;

            break;
         }

      case 5: // end session
         run = false;
         break;

      case 6: // read file path for local IO optimization
         self->m_DataChn.send(client_ip, client_port, transid, self->m_strHomeDir.c_str(), self->m_strHomeDir.length() + 1);
         break;

      case 7: // synchronize with the client, make sure write is correct
      {
         int32_t size = 0;
         if (self->m_DataChn.recv4(client_ip, client_port, transid, size) < 0)
            break;
         char* buf = NULL;
         if (self->m_DataChn.recv(client_ip, client_port, transid, buf, size) < 0)
            break;
         int32_t ts = 0;
         if (self->m_DataChn.recv4(client_ip, client_port, transid, ts) < 0)
            break;

         WriteLog log;
         log.deserialize(buf, size);
         delete [] buf;

         int32_t confirm = -1;
         if (writelog.compare(log))
            confirm = 1;

         writelog.clear();

         if (confirm > 0)
         {
            //synchronize timestamp
            utimbuf ut;
            ut.actime = ts;
            ut.modtime = ts;
            utime(filename.c_str(), &ut);
         }

         self->m_DataChn.send(client_ip, client_port, transid, (char*)&confirm, 4);

         break;
      }

      case 8: // specify up and down links
      {
         char* buf = NULL;
         int size = 136;
         if (self->m_DataChn.recv(client_ip, client_port, transid, buf, size) < 0)
            break;

         int32_t response = bWrite ? 0 : -1;
         if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
            break;
         if (response == -1)
            break;

         src_ip = buf;
         src_port = *(int32_t*)(buf + 64);
         dst_ip = buf + 68;
         dst_port = *(int32_t*)(buf + 132);
         delete [] buf;

         if (src_port > 0)
         {
            // connect to uplink in the write chain
            if (!self->m_DataChn.isConnected(src_ip, src_port))
               self->m_DataChn.connect(src_ip, src_port);
         }
         else
         {
            // first node in the chain, read from client
            src_ip = client_ip;
            src_port = client_port;
         }
         
         if (dst_port > 0)
         {
            //connect downlink in the write chain
            if (!self->m_DataChn.isConnected(dst_ip, dst_port))
               self->m_DataChn.connect(dst_ip, dst_port);
         }

         break;
      }

      default:
         break;
      }
   }

   // close local file
   fhandle.close();

   gettimeofday(&t2, 0);
   int duration = t2.tv_sec - t1.tv_sec;
   double avgRS = 0;
   double avgWS = 0;
   if (duration > 0)
   {
      avgRS = rb / duration * 8.0 / 1000000.0;
      avgWS = wb / duration * 8.0 / 1000000.0;
   }

   self->m_SectorLog << LogStringTag(LogTag::START, LogLevel::SCREEN) << "file server closed " << src_ip << " " << src_port << " " << avgWS << " " << avgRS << LogStringTag(LogTag::END);
   self->m_SectorLog << LogStringTag(LogTag::START, LogLevel::LEVEL_3) << "file server closed " << src_ip << " " << src_port << " " << avgWS << " " << avgRS << LogStringTag(LogTag::END);

   //report to master the task is completed
   self->report(master_ip, master_port, transid, sname, change);

   self->m_DataChn.send(client_ip, client_port, transid, (char*)&cmd, 4);
   if (key > 0)
      self->m_DataChn.remove(client_ip, client_port);

   // clear this transaction
   self->m_TransManager.updateSlave(transid, self->m_iSlaveID);

   // unlock the file
   self->m_pLocalFile->unlock(sname, key, mode);

   return NULL;
}

void* Slave::copy(void* p)
{
   Slave* self = ((Param3*)p)->serv_instance;
   int transid = ((Param3*)p)->transid;
   string src = ((Param3*)p)->src;
   string dst = ((Param3*)p)->dst;
   string master_ip = ((Param3*)p)->master_ip;
   int master_port = ((Param3*)p)->master_port;
   delete (Param3*)p;

   SNode tmp;
   if (self->m_pLocalFile->lookup(src.c_str(), tmp) >= 0)
   {
      //if file is local, copy directly
      //note that in this case, src != dst, therefore this is a regular "cp" command, not a system replication
      //TODO: check disk space

      self->createDir(dst.substr(0, dst.rfind('/')));
      string rhome = self->reviseSysCmdPath(self->m_strHomeDir);
      string rsrc = self->reviseSysCmdPath(src);
      string rdst = self->reviseSysCmdPath(dst);
      system(("cp " + rhome + src + " " + rhome + rdst).c_str());

      // if the file has been modified during the replication, remove this replica
      int type = (src == dst) ? 3 : 1;
      if (self->report(master_ip, master_port, transid, dst, type) < 0)
         system(("rm " + rhome + rdst).c_str());

      return NULL;
   }

   bool success = true;

   queue<string> tr;
   tr.push(src);

   while (!tr.empty())
   {
      string src_path = tr.front();
      tr.pop();

      // try list this path
      SectorMsg msg;
      msg.setType(101);
      msg.setKey(0);
      msg.setData(0, src_path.c_str(), src_path.length() + 1);

      Address addr;
      self->m_Routing.lookup(src_path, addr);

      if (self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, &msg, &msg) < 0)
      {
         success = false;
         break;
      }

      if (msg.getType() >= 0)
      {
         // if this is a directory, put all files and sub-drectories into the queue of files to be copied

         string filelist = msg.getData();
         unsigned int s = 0;
         while (s < filelist.length())
         {
            int t = filelist.find(';', s);
            SNode sn;
            sn.deserialize(filelist.substr(s, t - s).c_str());
            tr.push(src_path + "/" + sn.m_strName);
            s = t + 1;
         }

         continue;
      }

      // open the file and copy it to local
      msg.setType(110);
      msg.setKey(0);

      int32_t mode = 1;
      msg.setData(0, (char*)&mode, 4);
      int64_t reserve = 0;
      msg.setData(4, (char*)&reserve, 8);
      int32_t localport = self->m_DataChn.getPort();
      msg.setData(12, (char*)&localport, 4);
      msg.setData(16, "\0", 1);
      msg.setData(80, src_path.c_str(), src_path.length() + 1);

      if ((self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, &msg, &msg) < 0) || (msg.getType() < 0))
      {
         success = false;
         break;
      }

      int32_t session = *(int32_t*)msg.getData();
      int64_t size = *(int64_t*)(msg.getData() + 4);
      time_t ts = *(int64_t*)(msg.getData() + 12);

      string ip = msg.getData() + 24;
      int32_t port = *(int32_t*)(msg.getData() + 64 + 24);

      if (!self->m_DataChn.isConnected(ip, port))
      {
         if (self->m_DataChn.connect(ip, port) < 0)
         {
            success = false;
            break;
         }
      }

      // download command: 3
      int32_t cmd = 3;
      self->m_DataChn.send(ip, port, session, (char*)&cmd, 4);

      int64_t offset = 0;
      self->m_DataChn.send(ip, port, session, (char*)&offset, 8);

      int response = -1;
      if ((self->m_DataChn.recv4(ip, port, session, response) < 0) || (-1 == response))
      {
         success = false;
         break;
      }

      string dst_path = dst;
      if (src != src_path)
         dst_path += "/" + src_path.substr(src.length() + 1, src_path.length() - src.length() - 1);

      //copy to .tmp first, then move to real location
      self->createDir(string(".tmp") + dst_path.substr(0, dst_path.rfind('/')));

      fstream ofs;
      ofs.open((self->m_strHomeDir + ".tmp" + dst_path).c_str(), ios::out | ios::binary | ios::trunc);

      int64_t unit = 64000000; //send 64MB each time
      int64_t torecv = size;
      int64_t recd = 0;
      while (torecv > 0)
      {
         int64_t block = (torecv < unit) ? torecv : unit;
         if (self->m_DataChn.recvfile(ip, port, session, ofs, offset + recd, block) < 0)
            unlink((self->m_strHomeDir + ".tmp" + dst_path).c_str());

         recd += block;
         torecv -= block;
      }

      ofs.close();

      // update total received data size
      self->m_SlaveStat.updateIO(ip, size, +SlaveStat::SYS_IN);

      cmd = 5;
      self->m_DataChn.send(ip, port, session, (char*)&cmd, 4);
      self->m_DataChn.recv4(ip, port, session, cmd);

      if (src == dst)
      {
         //utime: update timestamp according to the original copy, for replica only; files created by "cp" have new timestamp
         utimbuf ut;
         ut.actime = ts;
         ut.modtime = ts;
         utime((self->m_strHomeDir + ".tmp" + dst_path).c_str(), &ut);
      }
   }

   string rhome = self->reviseSysCmdPath(self->m_strHomeDir);
   string rfile = self->reviseSysCmdPath(dst);
   if (success)
   {
      // move from temporary dir to the real dir when the copy is completed
      self->createDir(dst.substr(0, dst.rfind('/')));
      system(("mv " + rhome + ".tmp" + rfile + " " + rhome + rfile).c_str());
   }
   else
   {
      // failed, remove all temporary files
      system(("rm -rf " + rhome + ".tmp" + rfile).c_str());
   }

   // if the file has been modified during the replication, remove this replica
   int32_t type = (src == dst) ? +FileChangeType::FILE_UPDATE_REPLICA : +FileChangeType::FILE_UPDATE_NEW;
   if (self->report(master_ip, master_port, transid, dst, type) < 0)
      system(("rm " + rhome + rfile).c_str());

   // clear this transaction
   self->m_TransManager.updateSlave(transid, self->m_iSlaveID);

   return NULL;
}
