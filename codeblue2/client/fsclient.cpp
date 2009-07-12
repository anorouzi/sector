/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/


This file is part of Sector Client.

The Sector Client is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

The Sector Client is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 07/06/2009
*****************************************************************************/


#include <common.h>
#include <fsclient.h>
#include <iostream>

using namespace std;

SectorFile::SectorFile():
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
m_pcLocalPath(NULL)
{
   pthread_mutex_init(&m_FileLock, NULL);
}

SectorFile::~SectorFile()
{
   delete [] m_pcLocalPath;
   pthread_mutex_destroy(&m_FileLock);
}

int SectorFile::open(const string& filename, int mode)
{
   m_strFileName = revisePath(filename);

   SectorMsg msg;
   msg.setType(110); // open the file
   msg.setKey(g_iKey);

   int32_t m = mode;
   msg.setData(0, (char*)&m, 4);
   int32_t port = g_DataChn.getPort();
   msg.setData(4, (char*)&port, 4);
   msg.setData(8, m_strFileName.c_str(), m_strFileName.length() + 1);

   Address serv;
   g_Routing.lookup(m_strFileName, serv);
   if (g_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_CONNECTION;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   m_llSize = *(int64_t*)(msg.getData() + 72);
   m_llCurReadPos = m_llCurWritePos = 0;

   m_bRead = mode & 1;
   m_bWrite = mode & 2;
   m_bSecure = mode & 16;

   // check APPEND
   if (mode & 8)
      m_llCurWritePos = m_llSize;

   m_strSlaveIP = msg.getData();
   m_iSlaveDataPort = *(int*)(msg.getData() + 64);
   m_iSession = *(int*)(msg.getData() + 68);

   cerr << "open file " << filename << " " << m_strSlaveIP << " " << m_iSlaveDataPort << endl;
   if (g_DataChn.connect(m_strSlaveIP, m_iSlaveDataPort) < 0)
      return SectorError::E_CONNECTION;

   string localip;
   int localport;
   g_DataChn.getSelfAddr(m_strSlaveIP, m_iSlaveDataPort, localip, localport);

   if (m_strSlaveIP == localip)
   {
      // the file is on the same node, check if the file can be read directly
      int32_t cmd = 6;
      g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);
      int size = 0;
      if (g_DataChn.recv(m_strSlaveIP, m_iSlaveDataPort, m_iSession, m_pcLocalPath, size) > 0)
      {
         fstream test((m_pcLocalPath + filename).c_str(), ios::binary | ios::in);
         if (!test.bad() && !test.fail())
            m_bLocal = true;
      }
   }

   memcpy(m_pcKey, g_pcCryptoKey, 16);
   memcpy(m_pcIV, g_pcCryptoIV, 8);
   g_DataChn.setCryptoKey(m_strSlaveIP, m_iSlaveDataPort, m_pcKey, m_pcIV);

   if (m_bWrite)
      g_StatCache.insert(filename);

   return 0;
}

int64_t SectorFile::read(char* buf, const int64_t& size)
{
   CGuard fg(m_FileLock);

   int realsize = size;
   if (m_llCurReadPos + size > m_llSize)
      realsize = int(m_llSize - m_llCurReadPos);

   if (m_bLocal)
   {
      fstream ifs((m_pcLocalPath + m_strFileName).c_str(), ios::binary | ios::in);
      ifs.seekg(m_llCurReadPos);
      ifs.read(buf, realsize);
      ifs.close();
      m_llCurReadPos += realsize;
      return realsize;
   }

   // read command: 1
   int32_t cmd = 1;
   g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);

   int response = -1;
   if ((g_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response) < 0) || (-1 == response))
      return SectorError::E_CONNECTION;

   char req[16];
   *(int64_t*)req = m_llCurReadPos;
   *(int64_t*)(req + 8) = realsize;
   if (g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, req, 16) < 0)
      return SectorError::E_CONNECTION;

   char* tmp = NULL;
   int64_t recvsize = g_DataChn.recv(m_strSlaveIP, m_iSlaveDataPort, m_iSession, tmp, realsize, m_bSecure);
   if (recvsize > 0)
   {
      memcpy(buf, tmp, recvsize);
      m_llCurReadPos += recvsize;
   }
   delete [] tmp;

   return recvsize;
}

int64_t SectorFile::write(const char* buf, const int64_t& size)
{
   CGuard fg(m_FileLock);

   // write command: 2
   int32_t cmd = 2;
   g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);

   int response = -1;
   if ((g_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response) < 0) || (-1 == response))
      return SectorError::E_CONNECTION;

   char req[16];
   *(int64_t*)req = m_llCurWritePos;
   *(int64_t*)(req + 8) = size;

   if (g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, req, 16) < 0)
      return SectorError::E_CONNECTION;

   int64_t sentsize = g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, buf, size, m_bSecure);

   if (sentsize > 0)
   {
      m_llCurWritePos += sentsize;
      if (m_llCurWritePos > m_llSize)
         m_llSize = m_llCurWritePos;

      // update the file stat information in local cache, for correct stat() call
      g_StatCache.update(m_strFileName, CTimer::getTime(), m_llSize);
   }

   return sentsize;
}

int SectorFile::download(const char* localpath, const bool& cont)
{
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
   g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);

   int response = -1;
   if ((g_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response) < 0) || (-1 == response))
      return SectorError::E_CONNECTION;
   if (g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&offset, 8) < 0)
      return SectorError::E_CONNECTION;

   int64_t realsize = m_llSize - offset;

   int64_t unit = 64000000; //send 64MB each time
   int64_t torecv = realsize;
   int64_t recd = 0;
   while (torecv > 0)
   {
      int64_t block = (torecv < unit) ? torecv : unit;
      if (g_DataChn.recvfile(m_strSlaveIP, m_iSlaveDataPort, m_iSession, ofs, offset + recd, block, m_bSecure) < 0)
         break;

      recd += block;
      torecv -= block;
   }

   if (recd < realsize)
      return SectorError::E_CONNECTION;

   ofs.close();

   return 1;
}

int SectorFile::upload(const char* localpath, const bool& cont)
{
   CGuard fg(m_FileLock);

   fstream ifs;
   ifs.open(localpath, ios::in | ios::binary);

   if (ifs.fail() || ifs.bad())
      return SectorError::E_LOCALFILE;

   ifs.seekg(0, ios::end);
   int64_t size = ifs.tellg();
   ifs.seekg(0);

   // upload command: 4
   int32_t cmd = 4;
   g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);

   int response = -1;
   if ((g_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response) < 0) || (-1 == response))
      return SectorError::E_CONNECTION;

   if (g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&size, 8) < 0)
      return SectorError::E_CONNECTION;

   int64_t unit = 64000000; //send 64MB each time
   int64_t tosend = size;
   int64_t sent = 0;
   while (tosend > 0)
   {
      int64_t block = (tosend < unit) ? tosend : unit;
      if (g_DataChn.sendfile(m_strSlaveIP, m_iSlaveDataPort, m_iSession, ifs, sent, block, m_bSecure) < 0)
         break;

      sent += block;
      tosend -= block;
   }

   if (sent < size)
      return SectorError::E_CONNECTION;

   ifs.close();

   return 1;
}

int SectorFile::close()
{
   CGuard fg(m_FileLock);

   // file close command: 5
   int32_t cmd = 5;
   g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);
   int response;
   g_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response);

   //g_DataChn.releaseCoder();
   g_DataChn.remove(m_strSlaveIP, m_iSlaveDataPort);

   if (m_bWrite)
      g_StatCache.remove(m_strFileName);

   return 1;
}

int SectorFile::seekp(int64_t off, int pos)
{
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

   return 1;
}

int SectorFile::seekg(int64_t off, int pos)
{
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

   return 1;
}

int64_t SectorFile::tellp()
{
   return m_llCurWritePos;
}

int64_t SectorFile::tellg()
{
   return m_llCurReadPos;
}

bool SectorFile::eof()
{
   return (m_llCurReadPos >= m_llSize);
}
