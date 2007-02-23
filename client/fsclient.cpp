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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#include <fsclient.h>

using namespace std;


CCBFile* CFSClient::createFileHandle()
{
   CCBFile *f = NULL;

   try
   {
      f = new CCBFile;
   }
   catch (...)
   {
      return NULL;
   }

   f->m_pFSClient = this;
   f->m_iProtocol = m_iProtocol;

   return f;
}

void CFSClient::releaseFileHandle(CCBFile* f)
{
   delete f;
}

int CFSClient::stat(const string& filename, CFileAttr& attr)
{
   CCBMsg msg;
   msg.setType(101); // stat
   msg.setData(0, filename.c_str(), filename.length() + 1);
   msg.m_iDataLength = 4 + filename.length() + 1;

   if (m_pGMP->rpc(m_strServerHost.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() > 0)
      attr.deserialize(msg.getData(), msg.m_iDataLength - 4);

   return msg.getType();
}


CCBFile::CCBFile():
m_pFSClient(NULL)
{
   m_GMP.init(0);
}

CCBFile::~CCBFile()
{
   m_GMP.close();
}

int CCBFile::open(const string& filename, const int& mode, char* cert)
{
   m_strFileName = filename;

   Node n;

   if (m_pFSClient->lookup(filename, &n) < 0)
      return -1;

   CCBMsg msg;
   msg.setType(1); // locate file
   msg.setData(0, filename.c_str(), filename.length() + 1);
   msg.m_iDataLength = 4 + filename.length() + 1;

   if (m_GMP.rpc(n.m_pcIP, n.m_iAppPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() > 0)
   {
      int num = (msg.m_iDataLength - 4) / 68;

      cout << num << " copies found!" << endl;

      // choose closest server
      int c = 0;
      int rtt = 100000000;
      for (int i = 0; i < num; ++ i)
      {
         //cout << "RTT: " << msg.getData() + i * 68 << " " << *(int*)(msg.getData() + i * 68 + 64) << " " << m_GMP.rtt(msg.getData() + i * 68, *(int32_t*)(msg.getData() + i * 68 + 64)) << endl;
         int r = m_GMP.rtt(msg.getData() + i * 68, *(int32_t*)(msg.getData() + i * 68 + 64));
         if (r < rtt)
         {
            rtt = r;
            c = i;
         }
      }

      m_strServerIP = msg.getData() + c * 68;
      m_iServerPort = *(int32_t*)(msg.getData() + c * 68 + 64);
   }
   else
   {
      // file does not exist

      m_strServerIP = m_pFSClient->m_strServerHost;
      m_iServerPort = m_pFSClient->m_iServerPort;

      msg.setType(5); // create the file

      msg.setData(0, filename.c_str(), filename.length() + 1);
      msg.m_iDataLength = 4 + 64;

      if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
         return -1;

      //cout << "file owner certificate: " << msg.getData() << endl;
      if (NULL != cert)
         strcpy(cert, msg.getData());
   }

   msg.setType(2); // open the file
   msg.setData(0, filename.c_str(), filename.length() + 1);
   msg.setData(64, (char*)&m_iProtocol, 4);
   msg.setData(68, (char*)&mode, 4);
   if (NULL != cert)
   {
      msg.setData(72, cert, strlen(cert) + 1);
      msg.m_iDataLength = 4 + 64 + 4 + 4 + strlen(cert) + 1;
   }
   else
      msg.m_iDataLength = 4 + 64 + 4 + 4;

   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (1 == m_iProtocol)
   {
      m_uSock = UDT::socket(AF_INET, SOCK_STREAM, 0);

      #ifdef WIN32
         int mtu = 1052;
         UDT::setsockopt(m_uSock, 0, UDT_MSS, &mtu, sizeof(int));
      #endif
   }
   else
      m_tSock = ::socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = *(int*)(msg.getData()); // port
   #ifndef WIN32
      inet_pton(AF_INET, m_strServerIP.c_str(), &serv_addr.sin_addr);
   #else
      serv_addr.sin_addr.s_addr = inet_addr(m_strServerIP.c_str());
   #endif
      memset(&(serv_addr.sin_zero), '\0', 8);

   if (1 == m_iProtocol)
   {
      if (UDT::ERROR == UDT::connect(m_uSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
         return -1;

      //cout << "connect to UDT port " << *(int*)(msg.getData()) << endl;
   }
   else
   {
      if (-1 == ::connect(m_tSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
         return -1;

      //cout << "connect to TCP port " << *(int*)(msg.getData()) << endl;
   }

   //cout << "connected!\n";

   return 1;
}

int CCBFile::read(char* buf, const int64_t& offset, const int64_t& size)
{
   char req[20];
   *(int32_t*)req = 1; // cmd read
   *(int64_t*)(req + 4) = offset;
   *(int64_t*)(req + 12) = size;
   int32_t response = -1;

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, req, 20, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;

      int h;
      if (UDT::recv(m_uSock, buf, size, 0, &h) < 0)
         return -1;
   }
   else
   {
      if (::send(m_tSock, req, 20, 0) < 0)
         return -1;
      if ((::recv(m_tSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;

      int64_t rs = 0;
      while (rs < size)
      {
         int r = ::recv(m_tSock, buf, size, 0);
         if (r < 0)
            return -1;

         rs += r;
      }
   }

   return 1;
}

int CCBFile::write(const char* buf, const int64_t& offset, const int64_t& size)
{
   char req[20];
   *(int32_t*)req = 2; // cmd write
   *(int64_t*)(req + 4) = offset;
   *(int64_t*)(req + 12) = size;
   int32_t response = -1;

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, req, 20, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;

      int h;
      if (UDT::send(m_uSock, buf, size, 0, &h) < 0)
         return -1;
   }
   else
   {
      if (::send(m_tSock, req, 20, 0) < 0)
         return -1;
      if ((::recv(m_tSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;

      int64_t ss = 0;
      while (ss < size)
      {
         int s = ::send(m_tSock, buf, size, 0);
         if (s < 0)
            return -1;

         ss += s;
      }
   }

   return 1;
}

int CCBFile::download(const char* localpath, const bool& cont)
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

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, req, 12, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (UDT::recv(m_uSock, (char*)&size, 8, 0) < 0)
         return -1;

      if (UDT::recvfile(m_uSock, ofs, offset, size) < 0)
         return -1;
   }
   else
   {
      if (::send(m_tSock, req, 12, 0) < 0)
         return -1;
      if ((::recv(m_tSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;
      if (::recv(m_tSock, (char*)&size, 8, 0) < 0)
         return -1;

      int64_t rs = 0;
      char buf[4096];
      while (rs < size)
      {
         int r = ::recv(m_tSock, buf, 4096, 0);
         if (r < 0)
            return -1;

         ofs.write(buf, r);

         rs += r;
      }
   }

   ofs.close();

   return 1;
}

int CCBFile::upload(const char* localpath, const bool& cont)
{
   int32_t cmd = 5;
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

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, req, 12, 0) < 0)
         return -1;
      if ((UDT::recv(m_uSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;

      if (UDT::sendfile(m_uSock, ifs, 0, size) < 0)
         return -1;
   }
   else
   {
      if (::send(m_tSock, req, 12, 0) < 0)
         return -1;
      if ((::recv(m_tSock, (char*)&response, 4, 0) < 0) || (-1 == response))
         return -1;

      int unit = 10240000;
      char* data = new char[unit];
      int ssize = 0;

      while (ssize + unit <= size)
      {
         ifs.read(data, unit);

         int ts = 0;
         while (ts < unit)
         {
            int ss = ::send(m_tSock, data + ts, unit - ts, 0);
            if (ss < 0)
               goto ERROR;

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
            int ss = ::send(m_tSock, data + ssize, size - ssize, 0);
            if (ss < 0)
               goto ERROR;

            ts += ss;
         }
      }

      delete [] data;
   }

   ifs.close();

   return 1;

ERROR:
   perror("send");
   ifs.close();
   return -1;
}

int CCBFile::close()
{
   int32_t cmd = 4;

   if (1 == m_iProtocol)
   {
      if (UDT::send(m_uSock, (char*)&cmd, 4, 0) < 0)
         return -1;

      UDT::close(m_uSock);
   }
   else
   {
      if (::send(m_tSock, (char*)&cmd, 4, 0) < 0)
         return -1;

      closesocket(m_tSock);
   }

   return 1;
}
