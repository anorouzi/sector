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
   Yunhong Gu, last updated 10/25/2010
*****************************************************************************/


#include <common.h>
#include <fsclient.h>

using namespace std;

FSClient* Client::createFSClient()
{
   CGuard ig(m_IDLock);
   FSClient* sf = NULL;

   try
   {
      sf = new FSClient;
      sf->m_pClient = this;

      sf->m_iID = m_iID ++;
      m_mFSList[sf->m_iID] = sf;
   }
   catch (...)
   {
      delete sf;
      return NULL;
   }

   return sf;
}

int Client::releaseFSClient(FSClient* sf)
{
   CGuard::enterCS(m_IDLock);
   m_mFSList.erase(sf->m_iID);
   CGuard::leaveCS(m_IDLock);
   delete sf;

   return 0;
}

FSClient::FSClient():
m_iSession(),
m_strSlaveIP(),
m_iSlaveDataPort(),
m_strFileName(),
m_llSize(0),
m_llCurReadPos(0),
m_llCurWritePos(0),
m_bRead(false),
m_bWrite(false),
m_bSecure(false),
m_bLocal(false),
m_pcLocalPath(NULL),
m_iWriteBufSize(1000000),
m_WriteLog(),
m_llLastFlushTime(0),
m_bOpened(false)
{
   CGuard::createMutex(m_FileLock);
}

FSClient::~FSClient()
{
   m_bOpened = false;
   delete [] m_pcLocalPath;

   CGuard::releaseMutex(m_FileLock);
}

int FSClient::open(const string& filename, int mode, const SF_OPT* option)
{
   // if this client is already associated with an openned file, cannot open another file
   if (0 != m_strFileName.length())
      return SectorError::E_INVALID;

   m_strFileName = Metadata::revisePath(filename);

   SectorMsg msg;
   msg.setType(110); // open the file
   msg.setKey(m_pClient->m_iKey);

   int32_t m = mode;
   msg.setData(0, (char*)&m, 4);
   int32_t port = m_pClient->m_DataChn.getPort();
   msg.setData(4, (char*)&port, 4);
   int32_t len_name = m_strFileName.length() + 1;
   msg.setData(8, (char*)&len_name, 4);
   msg.setData(12, m_strFileName.c_str(), len_name);

   // send file open options
   int32_t len_opt = 0;
   string buf = "";
   if (NULL != option)
   {
      option->serialize(buf);
      len_opt = buf.length() + 1;
   }
   msg.setData(12 + len_name, (char*)&len_opt, 4);
   if (len_opt > 0)
      msg.setData(16 + len_name, buf.c_str(), len_opt);

   Address serv;
   if (m_pClient->lookup(m_strFileName, serv) < 0)
      return SectorError::E_MASTER;

   if ((m_pClient->m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0) || ((msg.getType() < 0) && (SectorError::E_ROUTING == *(int32_t*)msg.getData())))
   {
      // masters might have been changed, retrieve new master information
      m_pClient->updateMasters();
      if (m_pClient->lookup(m_strFileName, serv) < 0)
         return SectorError::E_MASTER;
      if (m_pClient->m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
         return SectorError::E_MASTER;
      if (msg.getType() < 0)
         return *(int32_t*)msg.getData();
   }
   else if (msg.getType() < 0)
   {
      return *(int32_t*)msg.getData();
   }

   m_iSession = *(int32_t*)msg.getData();

   m_llSize = *(int64_t*)(msg.getData() + 4);
   m_llTimeStamp = *(int64_t*)(msg.getData() + 12);
   m_llCurReadPos = m_llCurWritePos = 0;

   m_bRead = mode & 1;
   m_bWrite = mode & 2;
   m_bSecure = mode & 16;

   // check APPEND
   if (mode & 8)
      m_llCurWritePos = m_llSize;

   //TODO: handle TRUNC

   // receiving all replica nodes
   int32_t slave_num = *(int32_t*)(msg.getData() + 20);
   int offset = 24;

   m_vReplicaAddress.clear();
   for (int i = 0; i < slave_num; ++ i)
   {
      Address addr;
      addr.m_strIP = msg.getData() + offset;
      addr.m_iPort = *(int32_t*)(msg.getData() + offset + 64);
      offset += 68;
      if (m_pClient->m_DataChn.connect(addr.m_strIP, addr.m_iPort) >= 0)
         m_vReplicaAddress.push_back(addr);
   }

   while (m_bWrite && !m_vReplicaAddress.empty() && (organizeChainOfWrite() < 0)) {}

   if (m_vReplicaAddress.empty())
   {
      if (m_bWrite)
         return SectorError::E_CONNECTION;

      if (m_bRead && (reopen() < 0))
         return SectorError::E_CONNECTION;
   }

   m_strSlaveIP = m_vReplicaAddress.begin()->m_strIP;
   m_iSlaveDataPort = m_vReplicaAddress.begin()->m_iPort;

   string localip;
   int localport;
   m_pClient->m_DataChn.getSelfAddr(m_strSlaveIP, m_iSlaveDataPort, localip, localport);

   if (m_strSlaveIP == localip)
   {
      // the file is on the same node, check if the file can be read directly
      int32_t cmd = 6;
      m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);
      int size = 0;
      delete [] m_pcLocalPath;
      m_pcLocalPath = NULL;
      if (m_pClient->m_DataChn.recv(m_strSlaveIP, m_iSlaveDataPort, m_iSession, m_pcLocalPath, size) > 0)
      {
         fstream test((m_pcLocalPath + filename).c_str(), ios::binary | ios::in);
         if (!test.bad() && !test.fail())
            m_bLocal = true;
         test.close();
      }
   }

   // crypto key should only be set once per connection
   // repeated request to set the key will be ignored by datachn
   memcpy(m_pcKey, m_pClient->m_pcCryptoKey, 16);
   memcpy(m_pcIV, m_pClient->m_pcCryptoIV, 8);
   m_pClient->m_DataChn.setCryptoKey(m_strSlaveIP, m_iSlaveDataPort, m_pcKey, m_pcIV);

   m_pClient->m_Cache.update(m_strFileName, m_llTimeStamp, m_llSize, true);

   m_WriteLog.clear();

   m_bOpened = true;

   return 0;
}

int FSClient::reopen()
{
   if (0 == m_strFileName.length())
      return SectorError::E_INVALID;

   // re-open only works on read
   if (m_bWrite)
      return SectorError::E_PERMISSION;

   // clear current connection information
   m_strSlaveIP = "";
   m_iSlaveDataPort = 0;
   m_vReplicaAddress.clear();

   SectorMsg msg;
   msg.setType(112); // open the file
   msg.setKey(m_pClient->m_iKey);
   msg.setData(0, (char*)&m_iSession, 4);
   int32_t port = m_pClient->m_DataChn.getPort();
   msg.setData(4, (char*)&port, 4);

   Address serv;
   if (m_pClient->lookup(m_strFileName, serv) < 0)
      return SectorError::E_MASTER;
   if (m_pClient->m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   m_strSlaveIP = msg.getData();
   m_iSlaveDataPort = *(int*)(msg.getData() + 64);

   Address addr;
   addr.m_strIP = m_strSlaveIP;
   addr.m_iPort = m_iSlaveDataPort;
   m_vReplicaAddress.push_back(addr);

   if (m_pClient->m_DataChn.connect(m_strSlaveIP, m_iSlaveDataPort) < 0)
      return SectorError::E_CONNECTION;

   memcpy(m_pcKey, m_pClient->m_pcCryptoKey, 16);
   memcpy(m_pcIV, m_pClient->m_pcCryptoIV, 8);
   m_pClient->m_DataChn.setCryptoKey(m_strSlaveIP, m_iSlaveDataPort, m_pcKey, m_pcIV);

   return 0;
}

int64_t FSClient::read(char* buf, const int64_t& offset, const int64_t& size, const int64_t& prefetch)
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   if ((offset < 0) || (offset > m_llSize))
      return SectorError::E_INVALID;

   if (!m_bRead)
      return SectorError::E_PERMISSION;

   // does not support buffer > 32bit now
   if (size > 0x7FFFFFFF)
      return SectorError::E_INVALID;

   CGuard fg(m_FileLock);

   m_llCurReadPos = offset;

   int realsize = int(size);
   if (m_llCurReadPos + size > m_llSize)
      realsize = int(m_llSize - m_llCurReadPos);

   // optimization on local file; read directly outside Sector
   if (m_bLocal)
   {
      fstream ifs((m_pcLocalPath + m_strFileName).c_str(), ios::binary | ios::in);
      ifs.seekg(m_llCurReadPos);
      ifs.read(buf, realsize);
      ifs.close();
      m_llCurReadPos += realsize;
      return realsize;
   }

   // check cache
   int64_t cr = m_pClient->m_Cache.read(m_strFileName, buf, m_llCurReadPos, realsize);
   if (cr > 0)
   {
      m_llCurReadPos += cr;
      return cr;
   }
   if (prefetch >= size)
   {
      this->prefetch(m_llCurReadPos, prefetch);
      cr = m_pClient->m_Cache.read(m_strFileName, buf, m_llCurReadPos, realsize);
      if (cr > 0)
      {
         m_llCurReadPos += cr;
         return cr;
      }
   }

   // read command: 1
   int32_t cmd = 1;
   m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);

   char req[16];
   *(int64_t*)req = m_llCurReadPos;
   *(int64_t*)(req + 8) = realsize;
   m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, req, 16);

   int response = -1;
   if ((m_pClient->m_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response) < 0) || (-1 == response))
   {
      if (reopen() >= 0)
         return 0;
      return SectorError::E_CONNECTION;
   }

   char* tmp = NULL;
   int64_t recvsize = m_pClient->m_DataChn.recv(m_strSlaveIP, m_iSlaveDataPort, m_iSession, tmp, realsize, m_bSecure);
   if (recvsize > 0)
   {
      memcpy(buf, tmp, recvsize);
      m_llCurReadPos += recvsize;
      delete [] tmp;
   }
   else if (recvsize < 0)
   {
      if (reopen() >= 0)
         return 0;
   }

   return recvsize;
}

int64_t FSClient::write(const char* buf, const int64_t& offset, const int64_t& size, const int64_t& /*buffer*/)
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   if (offset < 0)
      return SectorError::E_INVALID;

   if (!m_bWrite)
      return SectorError::E_PERMISSION;

   if (size > 0x7FFFFFF)
      return SectorError::E_INVALID;

   CGuard fg(m_FileLock);

   m_llCurWritePos = offset;

   // send write msg from the end of the chain, so that if the client is broken, all replicas should be still the same
   for (vector<Address>::reverse_iterator i = m_vReplicaAddress.rbegin(); i != m_vReplicaAddress.rend(); ++ i)
   {
      // write command: 2
      int32_t cmd = 2;
      m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&cmd, 4);
   }

   // send offset and size parameters
   char req[16];
   *(int64_t*)req = m_llCurWritePos;
   *(int64_t*)(req + 8) = size;
   m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, req, 16);

   int64_t sentsize = m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, buf, size, m_bSecure);

   if (sentsize > 0)
   {
      m_llCurWritePos += sentsize;
      if (m_llCurWritePos > m_llSize)
         m_llSize = m_llCurWritePos;

      // update the file stat information in local cache, for correct stat() call and invalidate related read cache
      m_pClient->m_Cache.update(m_strFileName, CTimer::getTime(), m_llSize);

      // keep the data in cache, in case write is not completed
      char* data = new char[sentsize];
      memcpy(data, buf, sentsize);
      m_pClient->m_Cache.insert(data, m_strFileName, offset, sentsize, true);

      m_WriteLog.insert(offset, sentsize);
   }
   else
   {
      // write to the first replica failed, re-organize
      int result = flush_();
      if (result < 0)
         return result;
   }

   if (m_WriteLog.getCurrTotalSize() > m_iWriteBufSize)
   {
      int result = flush_();
      if (result < 0)
         return result;
   }

   return sentsize;
}

int64_t FSClient::read(char* buf, const int64_t& size)
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   // offset is a reference, so we create a copy here
   int64_t offset = m_llCurReadPos;
   return read(buf, offset, size);
}

int64_t FSClient::write(const char* buf, const int64_t& size)
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   int64_t offset = m_llCurWritePos;
   return write(buf, offset, size);
}

int64_t FSClient::download(const char* localpath, const bool& cont)
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   CGuard fg(m_FileLock);

   int64_t offset;
   fstream ofs;

   if (cont)
   {
      ofs.open(localpath, ios::out | ios::binary | ios::app);
      ofs.seekp(0, ios::end);
      offset = ofs.tellp();
   }
   else
   {
      ofs.open(localpath, ios::out | ios::binary | ios::trunc);
      offset = 0LL;
   }

   if (ofs.bad() || ofs.fail())
      return SectorError::E_LOCALFILE;

   // download command: 3
   int32_t cmd = 3;
   m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);

   m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&offset, 8);

   int response = -1;
   if ((m_pClient->m_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response) < 0) || (-1 == response))
      return SectorError::E_CONNECTION;

   int64_t realsize = m_llSize - offset;

   int64_t unit = 64000000; //send 64MB each time
   int64_t torecv = realsize;
   while (torecv > 0)
   {
      int64_t block = (torecv < unit) ? torecv : unit;
      if (m_pClient->m_DataChn.recvfile(m_strSlaveIP, m_iSlaveDataPort, m_iSession, ofs, m_llSize - torecv, block, m_bSecure) < 0)
         break;

      torecv -= block;
   }

   if (torecv > 0)
   {
      // retry once with another copy
      if (reopen() >= 0)
      {
         cmd = 3;
         m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);
         offset = ofs.tellp();
         m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&offset, 8);
         response = -1;
         if ((m_pClient->m_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response) < 0) || (-1 == response))
            return SectorError::E_CONNECTION;

         torecv = m_llSize - offset;
         while (torecv > 0)
         {
            int64_t block = (torecv < unit) ? torecv : unit;
            if (m_pClient->m_DataChn.recvfile(m_strSlaveIP, m_iSlaveDataPort, m_iSession, ofs, m_llSize - torecv, block, m_bSecure) < 0)
               break;

            torecv -= block;
         }
      }
   }

   ofs.close();

   if (torecv > 0)
      return SectorError::E_CONNECTION;

   return realsize;
}

int64_t FSClient::upload(const char* localpath, const bool& /*cont*/)
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   CGuard fg(m_FileLock);

   fstream ifs;
   ifs.open(localpath, ios::in | ios::binary);

   if (ifs.fail() || ifs.bad())
      return SectorError::E_LOCALFILE;

   ifs.seekg(0, ios::end);
   int64_t size = ifs.tellg();
   ifs.seekg(0);

   for (vector<Address>::reverse_iterator i = m_vReplicaAddress.rbegin(); i != m_vReplicaAddress.rend(); ++ i)
   {
      // upload command: 4
      int32_t cmd = 4;
      m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&cmd, 4);
      m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&size, 8);

      int response = -1;
      if ((m_pClient->m_DataChn.recv4(i->m_strIP, i->m_iPort, m_iSession, response) < 0) || (-1 == response))
         return SectorError::E_CONNECTION;
   }

   int64_t unit = 64000000; //send 64MB each time
   int64_t tosend = size;
   int64_t sent = 0;
   while (tosend > 0)
   {
      int64_t block = (tosend < unit) ? tosend : unit;
      if (m_pClient->m_DataChn.sendfile(m_strSlaveIP, m_iSlaveDataPort, m_iSession, ifs, sent, block, m_bSecure) < 0)
         break;

      sent += block;
      tosend -= block;
   }

   if (sent < size)
      return SectorError::E_CONNECTION;

   ifs.close();

   // check replica integrity
   m_WriteLog.insert(0, size);
   flush_();

   return size;
}

int FSClient::flush()
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   CGuard fg(m_FileLock);

   return flush_();
}

int FSClient::close()
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   CGuard fg(m_FileLock);

   flush_();

   for (vector<Address>::iterator i = m_vReplicaAddress.begin(); i != m_vReplicaAddress.end(); ++ i)
   {
      // file close command: 5
      int32_t cmd = 5;
      if (m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&cmd, 4) > 0)
      {
         int response;
         m_pClient->m_DataChn.recv4(i->m_strIP, i->m_iPort, m_iSession, response);
      }
   }

   //m_pClient->m_DataChn.releaseCoder();

   m_pClient->m_Cache.remove(m_strFileName);

   m_strFileName = "";

   delete [] m_pcLocalPath;
   m_pcLocalPath = NULL;

   return 0;
}

int64_t FSClient::seekp(int64_t off, int pos)
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   CGuard fg(m_FileLock);

   switch (pos)
   {
   case SF_POS::BEG:
      if (off < 0)
         return SectorError::E_INVALID;
      m_llCurWritePos = off;
      break;

   case SF_POS::CUR:
      if (off < -m_llCurWritePos)
         return SectorError::E_INVALID;
      m_llCurWritePos += off;
      break;

   case SF_POS::END:
      if (off < -m_llSize)
         return SectorError::E_INVALID;
      m_llCurWritePos = m_llSize + off;
      break;
   }

   return m_llCurWritePos;
}

int64_t FSClient::seekg(int64_t off, int pos)
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   CGuard fg(m_FileLock);

   switch (pos)
   {
   case SF_POS::BEG:
      if ((off < 0) || (off > m_llSize))
         return SectorError::E_INVALID;
      m_llCurReadPos = off;
      break;

   case SF_POS::CUR:
      if ((off < -m_llCurReadPos) || (off > m_llSize - m_llCurReadPos))
         return SectorError::E_INVALID;
      m_llCurReadPos += off;
      break;

   case SF_POS::END:
      if ((off < -m_llSize) || (off > 0))
         return SectorError::E_INVALID;
      m_llCurReadPos = m_llSize + off;
      break;
   }

   return m_llCurReadPos;
}

int64_t FSClient::tellp()
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   return m_llCurWritePos;
}

int64_t FSClient::tellg()
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   return m_llCurReadPos;
}

bool FSClient::eof()
{
   if (!m_bOpened)
      return SectorError::E_FILENOTOPEN;

   return (m_llCurReadPos >= m_llSize);
}

int64_t FSClient::prefetch(const int64_t& offset, const int64_t& size)
{
   int realsize = (int)size;
   if (offset >= m_llSize)
      return SectorError::E_INVALID;
   if (offset + size > m_llSize)
      realsize = int(m_llSize - offset);

   // read command: 1
   int32_t cmd = 1;
   m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);

   char req[16];
   *(int64_t*)req = offset;
   *(int64_t*)(req + 8) = realsize;
   m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, req, 16);

   int response = -1;
   if ((m_pClient->m_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response) < 0) || (-1 == response))
      return SectorError::E_CONNECTION;

   char* buf = NULL;
   int64_t recvsize = m_pClient->m_DataChn.recv(m_strSlaveIP, m_iSlaveDataPort, m_iSession, buf, realsize, m_bSecure);
   if (recvsize <= 0)
      return SectorError::E_CONNECTION;

   if (m_pClient->m_Cache.insert(buf, m_strFileName, offset, recvsize) < 0)
   {
      delete [] buf;
      return -1;
   }

   return recvsize;
}

int FSClient::organizeChainOfWrite()
{
   string src_ip = "";
   int src_port = -1;
   string dst_ip = "";
   int dst_port = -1;
   unsigned int i = 0;

   // chain of write: each slave node write to the next 
   // the first receive data from client, the last write to nobody

   for (vector<Address>::iterator r = m_vReplicaAddress.begin(); r != m_vReplicaAddress.end(); ++ r)
   {
      if (i > 0)
      {
         src_ip = m_vReplicaAddress[i - 1].m_strIP;
         src_port = m_vReplicaAddress[i - 1].m_iPort;
      }
      else
      {
         src_ip = "";
         src_port = -1;
      }

      if (i + 1 < m_vReplicaAddress.size())
      {
         dst_ip = m_vReplicaAddress[i + 1].m_strIP;
         dst_port = m_vReplicaAddress[i + 1].m_iPort;
      }
      else
      {
         dst_ip = "";
         dst_port = -1;
      }

      char buf[136];
      strcpy(buf, src_ip.c_str());
      *(int32_t*)(buf + 64) = src_port;
      strcpy(buf + 68, dst_ip.c_str());
      *(int32_t*)(buf + 132) = dst_port;

      int32_t cmd = 8;
      m_pClient->m_DataChn.send(r->m_strIP, r->m_iPort, m_iSession, (char*)&cmd, 4);
      m_pClient->m_DataChn.send(r->m_strIP, r->m_iPort, m_iSession, buf, 136);

      int32_t confirmation = -1;
      if ((m_pClient->m_DataChn.recv4(r->m_strIP, r->m_iPort, m_iSession, confirmation) < 0) || (confirmation < 0))
      {
         m_vReplicaAddress.erase(r);
         if (m_vReplicaAddress.empty())
         {
            m_strSlaveIP = "";
            m_iSlaveDataPort = 0;
         }
         else
         {
            m_strSlaveIP = m_vReplicaAddress.begin()->m_strIP;
            m_iSlaveDataPort = m_vReplicaAddress.begin()->m_iPort;
         }
         return -1;
      }

      ++ i;
   }

   return 0;
}

int FSClient::flush_()
{
   char* log = NULL;
   int32_t size = 0;
   m_WriteLog.serialize(log, size);

   // return if nothing to flush
   if (0 == size)
      return 0;

   vector<Address> newaddr;

   int32_t ts = int32_t(CTimer::getTime() / 1000000);

   for (vector<Address>::iterator i = m_vReplicaAddress.begin(); i != m_vReplicaAddress.end(); ++ i)
   {
      int32_t cmd = 7;
      m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&cmd, 4);
      m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&size, 4);
      m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, log, size);

      // synchronize timestamp
      m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&ts, 4);

      int32_t confirm = -1;
      if (m_pClient->m_DataChn.recv4(i->m_strIP, i->m_iPort, m_iSession, confirm) < 0)
      {
         // this replica has been lost
      }
      else if (confirm < 0)
      {
         // data transfer was not complete (due to lost of replica in the chain)
         
         char buf[136];
         buf[0] = '\0';
         *(int32_t*)(buf + 64) = -1;
         buf[68] = '\0';
         *(int32_t*)(buf + 132) = -1;

         int32_t cmd = 8;
         m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&cmd, 4);
         m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, buf, 136);

         int32_t response = -1;
         if ((m_pClient->m_DataChn.recv4(i->m_strIP, i->m_iPort, m_iSession, response) < 0) || (response < 0))
            continue;

         for (vector<WriteEntry>::iterator w = m_WriteLog.m_vListOfWrites.begin(); w != m_WriteLog.m_vListOfWrites.end(); ++ w)
         {
            char* buf = m_pClient->m_Cache.retrieve(m_strFileName, w->m_llOffset, w->m_llSize);
            if (NULL == buf)
            {
               // TODO: fatal error
               break;
            }

            // write command: 2
            cmd = 2;
            m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&cmd, 4);

            char req[16];
            *(int64_t*)req = w->m_llOffset;
            *(int64_t*)(req + 8) = w->m_llSize;
            m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, req, 16);

            if (m_pClient->m_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, buf, w->m_llSize, m_bSecure) < 0)
               break;
         }


         // synchronize again for the rewrite
         cmd = 7;
         m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&cmd, 4);
         m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&size, 4);
         m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, log, size);
         m_pClient->m_DataChn.send(i->m_strIP, i->m_iPort, m_iSession, (char*)&ts, 4);

         if ((m_pClient->m_DataChn.recv4(i->m_strIP, i->m_iPort, m_iSession, confirm) > 0) && (confirm > 0))
         {
            // write is successful
            newaddr.push_back(*i);
         }
      }
      else
      {
         // write was successful
         newaddr.push_back(*i);
      }
   }

   delete [] log;

   // write has been synchronized
   for (vector<WriteEntry>::iterator w = m_WriteLog.m_vListOfWrites.begin(); w != m_WriteLog.m_vListOfWrites.end(); ++ w)
   {
      m_pClient->m_Cache.clearWrite(m_strFileName, w->m_llOffset, w->m_llSize);
   }
   m_WriteLog.clear();
   m_llLastFlushTime = CTimer::getTime();

   // all nodes are down, no way to recover
   if (newaddr.empty())
      return SectorError::E_NOREPLICA;

   // if some nodes are down, use the rest to finish write
   if (m_vReplicaAddress.size() != newaddr.size())
   {
      m_vReplicaAddress = newaddr;
      while (!m_vReplicaAddress.empty() && (organizeChainOfWrite() < 0)) {}
   }

   return 0;
}
