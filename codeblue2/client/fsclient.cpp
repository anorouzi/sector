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
   Yunhong Gu [gu@lac.uic.edu], last updated 03/12/2009
*****************************************************************************/


#include <fsclient.h>
#include <iostream>

using namespace std;

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

   if (g_GMP.rpc(g_strServerIP.c_str(), g_iServerPort, &msg, &msg) < 0)
      return SectorError::E_CONNECTION;
   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   m_llSize = *(int64_t*)(msg.getData() + 72);
   m_llCurReadPos = m_llCurWritePos = 0;

   m_bRead = mode & 1;
   m_bWrite = mode & 2;
   m_bSecure = mode & 16;

   m_strSlaveIP = msg.getData();
   m_iSlaveDataPort = *(int*)(msg.getData() + 64);
   m_iSession = *(int*)(msg.getData() + 68);

   cerr << "open file " << filename << " " << m_strSlaveIP << " " << m_iSlaveDataPort << endl;
   g_DataChn.connect(m_strSlaveIP, m_iSlaveDataPort);

   memcpy(m_pcKey, g_pcCryptoKey, 16);
   memcpy(m_pcIV, g_pcCryptoIV, 8);
   g_DataChn.setCryptoKey(m_strSlaveIP, m_iSlaveDataPort, m_pcKey, m_pcIV);

   return 0;
}

int64_t SectorFile::read(char* buf, const int64_t& size)
{
   int realsize = size;
   if (m_llCurReadPos + size > m_llSize)
      realsize = int(m_llSize - m_llCurReadPos);

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
      m_llCurWritePos += sentsize;

   return sentsize;
}

int SectorFile::download(const char* localpath, const bool& cont)
{
   int64_t offset;
   ofstream ofs;

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
   if (g_DataChn.recvfile(m_strSlaveIP, m_iSlaveDataPort, m_iSession, ofs, offset, realsize, m_bSecure) < 0)
      return SectorError::E_CONNECTION;

   ofs.close();

   return 1;
}

int SectorFile::upload(const char* localpath, const bool& cont)
{
   ifstream ifs;
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

   if (g_DataChn.sendfile(m_strSlaveIP, m_iSlaveDataPort, m_iSession, ifs, 0, size, m_bSecure) < 0)
      return SectorError::E_CONNECTION;

   ifs.close();

   return 1;
}

int SectorFile::close()
{
   // file close command: 5
   int32_t cmd = 5;
   g_DataChn.send(m_strSlaveIP, m_iSlaveDataPort, m_iSession, (char*)&cmd, 4);
   int response;
   g_DataChn.recv4(m_strSlaveIP, m_iSlaveDataPort, m_iSession, response);

   //g_DataChn.releaseCoder();
   g_DataChn.remove(m_strSlaveIP, m_iSlaveDataPort);

   return 1;
}

int SectorFile::seekp(int64_t off, int pos)
{
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
