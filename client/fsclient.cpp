/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#include <fsclient.h>

using namespace std;
using namespace cb;

File* Client::createFileHandle()
{
   File *f = NULL;

   try
   {
      f = new File;
   }
   catch (...)
   {
      return NULL;
   }

   return f;
}

void Client::releaseFileHandle(File* f)
{
   delete f;
}

int Client::stat(const string& filename, CFileAttr& attr)
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


File::File()
{
   m_GMP.init(0);
}

File::~File()
{
   m_GMP.close();
}

int File::open(const string& filename, const int& mode, char* cert, char* nl, int nlsize)
{
   m_strFileName = filename;

   CCBMsg msg;

   while (NULL == nl)
   {
      Node n;
      if (Client::lookup(filename, &n) < 0)
         break;

      msg.setType(1); // locate file
      msg.setData(0, filename.c_str(), filename.length() + 1);
      msg.m_iDataLength = 4 + filename.length() + 1;

      if (m_GMP.rpc(n.m_pcIP, n.m_iAppPort, &msg, &msg) < 0)
         break;

      nl = msg.getData();
      nlsize = (msg.m_iDataLength - 4) / 68;
cout << "GOT IT " << nl << endl;
      break;
   };

   bool serv_found = false;
   if (NULL != nl)
   {
      cout << nlsize << " copies found!" << endl;

      // choose closest server
      int c = -1;
      int rtt = 100000000;
      for (int i = 0; i < nlsize; ++ i)
      {
         int r = m_GMP.rtt(nl + i * 68, *(int32_t*)(nl + i * 68 + 64));
         if (r < rtt)
         {
            rtt = r;
            c = i;
         }
      }

      cout << "RTT " << nl << " " << *(int32_t*)(nl + 64) << " " << c << endl;

      if (-1 != c)
      {
         serv_found = true;
         m_strServerIP = nl + c * 68;
         m_iServerPort = *(int32_t*)(nl + c * 68 + 64);
      }
   }

   if (!serv_found)
   {
      // file does not exist
      if (1 == mode)
         return -1;

      m_strServerIP = Client::m_strServerHost;
      m_iServerPort = Client::m_iServerPort;

      CCBMsg msg;
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
   msg.setData(64, (char*)&mode, 4);

   int port = 0;
   m_DataChn.open(port);

   msg.setData(68, (char*)&port, 4);

   if (NULL != cert)
   {
      msg.setData(72, cert, strlen(cert) + 1);
      msg.m_iDataLength = 4 + 64 + 4 + 4 + strlen(cert) + 1;
   }
   else
      msg.m_iDataLength = 4 + 64 + 4 + 4;

   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   cout << "rendezvous connect " << m_strServerIP << " " << *(int*)(msg.getData()) << endl;

   return m_DataChn.connect(m_strServerIP.c_str(), *(int*)(msg.getData()));
}

int File::read(char* buf, const int64_t& offset, const int64_t& size)
{
   char req[20];
   *(int32_t*)req = 1; // cmd read
   *(int64_t*)(req + 4) = offset;
   *(int64_t*)(req + 12) = size;
   int32_t response = -1;

   if (m_DataChn.send(req, 20) < 0)
      return -1;
   if ((m_DataChn.recv((char*)&response, 4) < 0) || (-1 == response))
      return -1;

   if (m_DataChn.recv(buf, size) < 0)
      return -1;

   return 1;
}

int File::readridx(char* index, const int64_t& offset, const int64_t& rows)
{
   char req[20];
   *(int32_t*)req = 6; // cmd readrows
   *(int64_t*)(req + 4) = offset;
   *(int64_t*)(req + 12) = rows;
   int32_t response = -1;

   if (m_DataChn.send(req, 20) < 0)
      return -1;
   if ((m_DataChn.recv((char*)&response, 4) < 0) || (-1 == response))
      return -1;

   if (m_DataChn.recv(index, (rows + 1) * 8) < 0)
      return -1;

   return 1;
}

int File::write(const char* buf, const int64_t& offset, const int64_t& size)
{
   char req[20];
   *(int32_t*)req = 2; // cmd write
   *(int64_t*)(req + 4) = offset;
   *(int64_t*)(req + 12) = size;
   int32_t response = -1;

   if (m_DataChn.send(req, 20) < 0)
      return -1;
   if ((m_DataChn.recv((char*)&response, 4) < 0) || (-1 == response))
      return -1;

   if (m_DataChn.send(buf, size) < 0)
      return -1;

   return 1;
}

int File::download(const char* localpath, const bool& cont)
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

int File::upload(const char* localpath, const bool& cont)
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

int File::close()
{
   int32_t cmd = 5;

   if (m_DataChn.send((char*)&cmd, 4) < 0)
      return -1;

   m_DataChn.close();

   return 1;
}
