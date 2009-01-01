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
   Yunhong Gu [gu@lac.uic.edu], last updated 12/28/2008
*****************************************************************************/


#include <fsclient.h>
#include <iostream>

using namespace std;

int SectorFile::open(const string& filename, SF_MODE mode)
{
   m_strFileName = revisePath(filename);

   SectorMsg msg;
   msg.setType(110); // open the file
   msg.setKey(g_iKey);

   memcpy(m_pcKey, g_pcCryptoKey, 16);
   memcpy(m_pcIV, g_pcCryptoIV, 8);
   m_DataChn.initCoder(m_pcKey, m_pcIV);

   int port = g_iReusePort;
   m_DataChn.open(port, true, true);
   g_iReusePort = port;

   msg.setData(0, (char*)&port, 4);
   int32_t m = mode;
   msg.setData(4, (char*)&m, 4);
   msg.setData(8, m_strFileName.c_str(), m_strFileName.length() + 1);

   if (g_GMP.rpc(g_strServerIP.c_str(), g_iServerPort, &msg, &msg) < 0)
      return -1;
   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   m_llSize = *(int64_t*)(msg.getData() + 72);
   m_llCurReadPos = m_llCurWritePos = 0;

   cout << "open file " << filename << " " << msg.getData() << " " << *(int*)(msg.getData() + 64) << endl;
   return m_DataChn.connect(msg.getData(), *(int*)(msg.getData() + 68));
}

int64_t SectorFile::read(char* buf, const int64_t& size)
{
   char req[20];
   *(int32_t*)req = 1; // cmd read
   *(int64_t*)(req + 4) = m_llCurReadPos;
   *(int64_t*)(req + 12) = size;

   int32_t response = -1;
   int64_t rsize = -1;

   if (m_DataChn.send(req, 20) < 0)
      return -1;
   if ((m_DataChn.recv((char*)&response, 4) < 0) || (-1 == response))
      return -1;

   rsize = m_DataChn.recv(buf, size);

   if (rsize > 0)
      m_llCurReadPos += rsize;

   return rsize;
}

int64_t SectorFile::write(const char* buf, const int64_t& size)
{
   char req[20];
   *(int32_t*)req = 2; // cmd write
   *(int64_t*)(req + 4) = m_llCurWritePos;
   *(int64_t*)(req + 12) = size;

   int32_t response = -1;
   int64_t wsize = -1;

   if (m_DataChn.send(req, 20) < 0)
      return -1;
   if ((m_DataChn.recv((char*)&response, 4) < 0) || (-1 == response))
      return -1;

   wsize = m_DataChn.send(buf, size);

   if (wsize > 0)
      m_llCurWritePos += wsize;

   return wsize;
}

int SectorFile::download(const char* localpath, const bool& cont)
{
   int32_t cmd = 3;
   int64_t offset;
   int64_t size;
   int32_t response = -1;

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

   char req[12];
   *(int32_t*)req = cmd;
   *(int64_t*)(req + 4) = offset;

   if (m_DataChn.send(req, 12) < 0)
      return -1;
   if ((m_DataChn.recv((char*)&response, 4) < 0) || (-1 == response))
      return -1;
   if (m_DataChn.recv((char*)&size, 8) < 0)
      return -1;

   if (m_DataChn.recvfile(ofs, offset, size) < 0)
      return -1;

   ofs.close();

   return 1;
}

int SectorFile::upload(const char* localpath, const bool& cont)
{
   int32_t cmd = 4;
   int64_t size;
   int32_t response = -1;

   ifstream ifs;
   ifs.open(localpath, ios::in | ios::binary);

   ifs.seekg(0, ios::end);
   size = ifs.tellg();
   ifs.seekg(0);

   char req[12];
   *(int32_t*)req = cmd;
   *(int64_t*)(req + 4) = size;

   if (m_DataChn.send(req, 12) < 0)
      return -1;

   if ((m_DataChn.recv((char*)&response, 4) < 0) || (-1 == response))
      return -1;

   if (m_DataChn.sendfile(ifs, 0, size) < 0)
      return -1;

   ifs.close();

   return 1;
}

int SectorFile::close()
{
   int32_t cmd = 5;

   if (m_DataChn.send((char*)&cmd, 4) < 0)
      return -1;

   // wait for response
   m_DataChn.recv((char*)&cmd, 4);

   m_DataChn.releaseCoder();
   m_DataChn.close();

   return 1;
}

int SectorFile::seekp(int64_t off, SF_POS pos)
{
   switch (pos)
   {
   case BEG:
      if (off < 0)
         return -1;
      m_llCurWritePos = off;
      break;

   case CUR:
      if (off < -m_llCurWritePos)
         return -1;
      m_llCurWritePos += off;
      break;

   case END:
      if (off < -m_llSize)
         return -1;
      m_llCurWritePos = m_llSize + off;
      break;
   }

   return 0;
}

int SectorFile::seekg(int64_t off, SF_POS pos)
{
   switch (pos)
   {
   case BEG:
      if ((off < 0) || (off > m_llSize))
         return -1;
      m_llCurReadPos = off;
      break;

   case CUR:
      if ((off < -m_llCurReadPos) || (off > m_llSize - m_llCurReadPos))
         return -1;
      m_llCurReadPos += off;
      break;

   case END:
      if ((off < -m_llSize) || (off > 0))
         return -1;
      m_llCurReadPos = m_llSize + off;
      break;
   }

   return 0;
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
