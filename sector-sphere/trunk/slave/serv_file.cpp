/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 04/14/2011
*****************************************************************************/


#include <writelog.h>
#include <slave.h>
#ifndef WIN32
   #include <utime.h>
#else
   #include <sys/types.h>
   #include <sys/utime.h>
#endif
#include <iostream>
using namespace std;

#ifndef WIN32
void* Slave::fileHandler(void* p)
#else
DWORD WINAPI Slave::fileHandler(LPVOID p)
#endif
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
   string src_ip = client_ip;
   int src_port = client_port;
   string dst_ip;
   int dst_port = -1;

   // IO permissions
   bool bRead = mode & 1;
   bool bWrite = mode & 2;
   bool bTrunc = mode & 4;
   bool bSecure = mode & 16;

   int64_t orig_size = -1;
   int64_t orig_ts = -1;
   bool file_change = false;

   int last_timestamp = 0;

   self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "connecting to " << client_ip << " " << client_port << " " << filename << LogEnd();

   if ((!self->m_DataChn.isConnected(client_ip, client_port)) && (self->m_DataChn.connect(client_ip, client_port) < 0))
   {
      self->m_SectorLog << LogStart(LogLevel::LEVEL_2) << "failed to connect to file client " << client_ip << " " << client_port << " " << filename << LogEnd();

      // release transactions and file locks
      self->m_TransManager.updateSlave(transid, self->m_iSlaveID);
      self->m_pLocalFile->unlock(sname, key, mode);
      self->report(master_ip, master_port, transid, sname, +FileChangeType::FILE_UPDATE_NO);

      return NULL;
   }

   Crypto* encoder = NULL;
   Crypto* decoder = NULL;
   if (bSecure)
   {
      encoder = new Crypto;
      encoder->initEnc(crypto_key, crypto_iv);
      decoder = new Crypto;
      decoder->initDec(crypto_key, crypto_iv);      
   }

   //create a new directory or file in case it does not exist
   if (bWrite)
   {
      self->createDir(sname.substr(0, sname.rfind('/')));

      SNode s;
      if (LocalFS::stat(filename, s) < 0)
      {
         ofstream newfile(filename.c_str(), ios::out | ios::binary | ios::trunc);
         newfile.close();
      }
      else
      {
         orig_size = s.m_llSize;
         orig_ts = s.m_llTimeStamp;
      }
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);
   int64_t rb = 0;
   int64_t wb = 0;

   WriteLog writelog;

   fstream fhandle;
   if (!bTrunc)
      fhandle.open(filename.c_str(), ios::in | ios::out | ios::binary);
   else
      fhandle.open(filename.c_str(), ios::in | ios::out | ios::binary | ios::trunc);

   // a file session is successful only if the client issue a close() request
   bool success = true;
   bool run = true;
   int32_t cmd = 0;

   while (run)
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
               success = false;
               break;
            }
            int64_t offset = *(int64_t*)param;
            int64_t size = *(int64_t*)(param + 8);
            delete [] param;

            int32_t response = bRead ? 0 : -1;
            if (fhandle.fail() || !success || !self->m_bDiskHealth || !self->m_bNetworkHealth)
               response = -1;

            if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
               break;
            if (response == -1)
               break;

            if (self->m_DataChn.sendfile(client_ip, client_port, transid, fhandle, offset, size, encoder) < 0)
               success = false;
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
               success = false;
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
            Crypto* tmp_decoder = decoder;
            if ((client_ip != src_ip) || (client_port != src_port))
               tmp_decoder = NULL;

            bool io_status = (size > 0); 
            if (!io_status || (self->m_DataChn.recvfile(src_ip, src_port, transid, fhandle, offset, size, tmp_decoder) < size))
               io_status = false;

            //TODO: send incomplete write to next slave on chain, rather than -1

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

            file_change = true;

            break;
         }

      case 3: // download
         {
            int64_t offset;
            if (self->m_DataChn.recv8(client_ip, client_port, transid, offset) < 0)
            {
               success = false;
               break;
            }

            int32_t response = bRead ? 0 : -1;
            if (fhandle.fail() || !success || !self->m_bDiskHealth || !self->m_bNetworkHealth)
               response = -1;
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
               if (self->m_DataChn.sendfile(client_ip, client_port, transid, fhandle, offset + sent, block, encoder) < 0)
               {
                  success = false;
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
               success = false;
               break;
            }

            int64_t offset = 0;
            int64_t size;
            if (self->m_DataChn.recv8(client_ip, client_port, transid, size) < 0)
            {
               success = false;
               break;
            }

            //TODO: check available size
            int32_t response = 0;
            if (fhandle.fail() || !success || !self->m_bDiskHealth || !self->m_bNetworkHealth)
               response = -1;
            if (self->m_DataChn.send(client_ip, client_port, transid, (char*)&response, 4) < 0)
               break;
            if (response == -1)
               break;

            int64_t unit = 64000000; //send 64MB each time
            int64_t torecv = size;
            int64_t recd = 0;

            // no secure transfer between two slaves
            Crypto* tmp_decoder = decoder;
            if ((client_ip != src_ip) || (client_port != src_port))
               tmp_decoder = NULL;

            while (torecv > 0)
            {
               int64_t block = (torecv < unit) ? torecv : unit;

               if (self->m_DataChn.recvfile(src_ip, src_port, transid, fhandle, offset + recd, block, tmp_decoder) < 0)
               {
                  success = false;
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

            file_change = true;

            break;
         }

      case 5: // end session
         // the file has been successfully closed
         run = false;
         break;

      case 6: // read file path for local IO optimization
         self->m_DataChn.send(client_ip, client_port, transid, self->m_strHomeDir.c_str(), self->m_strHomeDir.length() + 1);
         break;

      case 7: // synchronize with the client, make sure write is correct
      {
         //TODO: merge all three recv() to one
         int32_t size = 0;
         if (self->m_DataChn.recv4(client_ip, client_port, transid, size) < 0)
            break;
         char* buf = NULL;
         if (self->m_DataChn.recv(client_ip, client_port, transid, buf, size) < 0)
            break;
         last_timestamp = 0;
         if (self->m_DataChn.recv4(client_ip, client_port, transid, last_timestamp) < 0)
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
            ut.actime = last_timestamp;
            ut.modtime = last_timestamp;
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
         if (fhandle.fail() || !success || !self->m_bDiskHealth || !self->m_bNetworkHealth)
            response = -1;
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

   // update final timestamp
   if (last_timestamp > 0)
   {
      utimbuf ut;
      ut.actime = last_timestamp;
      ut.modtime = last_timestamp;
      utime(filename.c_str(), &ut);
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

   self->m_SectorLog << LogStart(LogLevel::LEVEL_3) << "file server closed " << src_ip << " " << src_port << " " << (long long)avgWS << " " << (long long)avgRS << LogEnd();

   // clear this transaction
   self->m_TransManager.updateSlave(transid, self->m_iSlaveID);

   // unlock the file
   // this must be done before the client is disconnected, otherwise if the client immediately re-open the file, the lock may not be released yet
   self->m_pLocalFile->unlock(sname, key, mode);

   // report to master the task is completed
   // this also must be done before the client is disconnected, otherwise client may not be able to immediately re-open the file as the master is not updated
   if (bWrite)
   {
      // File update can be optimized outside Sector if the write is from local
      // thus the slave will not be able to know if the file has been changed, unless it checks the content
      // we check file size and timestamp here, but this is actually not enough, especially the time stamp granularity is too low
      SNode s;
      LocalFS::stat(filename, s);
      if ((s.m_llSize != orig_size) || (s.m_llTimeStamp != orig_ts))
         file_change = true;
cout << "hoho " << file_change << " " << s.m_llSize << " " << orig_size << endl;

   }
   int change = file_change ? +FileChangeType::FILE_UPDATE_WRITE : +FileChangeType::FILE_UPDATE_NO;

   self->report(master_ip, master_port, transid, sname, change);

   if (bSecure)
   {
      encoder->release();
      delete encoder;
      decoder->release();
      delete decoder;
   }

   if (success)
      self->m_DataChn.send(client_ip, client_port, transid, (char*)&cmd, 4);
   else
      self->m_DataChn.sendError(client_ip, client_port, transid);

   return NULL;
}

#ifndef WIN32
void* Slave::copy(void* p)
#else
DWORD WINAPI Slave::copy(LPVOID p)
#endif
{
   Slave* self = ((Param3*)p)->serv_instance;
   int transid = ((Param3*)p)->transid;
   int dir = ((Param3*)p)->dir;
   string src = ((Param3*)p)->src;
   string dst = ((Param3*)p)->dst;
   string master_ip = ((Param3*)p)->master_ip;
   int master_port = ((Param3*)p)->master_port;
   delete (Param3*)p;

   if (src.c_str()[0] == '\0')
      src = "/" + src;
   if (dst.c_str()[0] == '\0')
      dst = "/" + dst;

   bool success = true;

   queue<string> tr;	// files to be replicated
   queue<string> td;	// directories to be explored

   if (dir > 0)
      td.push(src);
   else
      tr.push(src);

   while (!td.empty())
   {
      // If the file to be replicated is a directory, recursively list all files first

      string src_path = td.front();
      td.pop();

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

      // the master only returns positive if this is a directory
      if (msg.getType() >= 0)
      {
         // if this is a directory, create it, and put all files and sub-directories into the queue of files to be copied

         // create a local dir
         string dst_path = dst;
         if (src != src_path)
            dst_path += "/" + src_path.substr(src.length() + 1, src_path.length() - src.length() - 1);

         //create at .tmp first, then move to real location
         self->createDir(string(".tmp") + dst_path);

         string filelist = msg.getData();
         unsigned int s = 0;
         while (s < filelist.length())
         {
            int t = filelist.find(';', s);
            SNode sn;
            sn.deserialize(filelist.substr(s, t - s).c_str());
            if (sn.m_bIsDir)
               td.push(src_path + "/" + sn.m_strName);
            else
               tr.push(src_path + "/" + sn.m_strName);
            s = t + 1;
         }

         continue;
      }
   }

   while (!tr.empty())
   {
      string src_path = tr.front();
      tr.pop();

      SNode tmp;
      if (self->m_pLocalFile->lookup(src_path.c_str(), tmp) >= 0)
      {
         //if file is local, copy directly
         //note that in this case, src != dst, therefore this is a regular "cp" command, not a system replication

         //IMPORTANT!!!
         //local files must be read directly from local disk, and cannot be read via datachn due to its limitation

         string dst_path = dst;
         if (src != src_path)
            dst_path += "/" + src_path.substr(src.length() + 1, src_path.length() - src.length() - 1);

         //copy to .tmp first, then move to real location
         self->createDir(string(".tmp") + dst_path.substr(0, dst_path.rfind('/')));
         LocalFS::copy(self->m_strHomeDir + src_path, self->m_strHomeDir + ".tmp" + dst_path);
      }
      else
      {
         // open the file and copy it to local
         SectorMsg msg;
         msg.setType(110);
         msg.setKey(0);

         int32_t mode = SF_MODE::READ;
         msg.setData(0, (char*)&mode, 4);
         int32_t localport = self->m_DataChn.getPort();
         msg.setData(4, (char*)&localport, 4);
         int32_t len_name = src_path.length() + 1;
         msg.setData(8, (char*)&len_name, 4);
         msg.setData(12, src_path.c_str(), len_name);
         int32_t len_opt = 0;
         msg.setData(12 + len_name, (char*)&len_opt, 4);

         Address addr;
         self->m_Routing.lookup(src_path, addr);

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
            {
               success = false;
               break;
            }

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
   }

   if (success)
   {
      // move from temporary dir to the real dir when the copy is completed
      self->createDir(dst.substr(0, dst.rfind('/')));
      LocalFS::rename(self->m_strHomeDir + ".tmp" + dst, self->m_strHomeDir + dst);

      // if the file has been modified during the replication, remove this replica
      int32_t type = (src == dst) ? +FileChangeType::FILE_UPDATE_REPLICA : +FileChangeType::FILE_UPDATE_NEW;
      if (self->report(master_ip, master_port, transid, dst, type) < 0)
         LocalFS::erase(self->m_strHomeDir + dst);
   }
   else
   {
      // failed, remove all temporary files
      LocalFS::erase(self->m_strHomeDir + ".tmp" + dst);
      self->report(master_ip, master_port, transid, "", +FileChangeType::FILE_UPDATE_NO);
   }

   // clear this transaction
   self->m_TransManager.updateSlave(transid, self->m_iSlaveID);

   return NULL;
}
