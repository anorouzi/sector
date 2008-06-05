/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/04/2008
*****************************************************************************/


#include <ssltransport.h>
#include <netdb.h>
#include "client.h"
#include <iostream>

using namespace std;

string Client::m_strServerHost = "";
string Client::m_strServerIP = "";
int Client::m_iServerPort = 0;
CGMP Client::m_GMP;
int32_t Client::m_iKey = 0;
int Client::m_iCount = 0;

Client::Client()
{
}

Client::~Client()
{
}

int Client::init(const string& server, const int& port)
{
   if (m_iCount ++ > 0)
      return 1;

   struct hostent* serverip = gethostbyname(server.c_str());
   if (NULL == serverip)
      return -1;

   m_strServerHost = server;
   char buf[64];
   m_strServerIP = inet_ntop(AF_INET, serverip->h_addr_list[0], buf, 64);
   
   m_iServerPort = port;

   m_GMP.init(0);

   return 1;
}

int Client::login(const string& username, const string& password)
{
   SSLTransport::init();

   SSLTransport secconn;
   secconn.initClientCTX("master_node.cert");
   secconn.open(NULL, 0);
   int r = secconn.connect(m_strServerHost.c_str(), m_iServerPort);

   if (r < 0)
      return -1;

   cout << "SEC CONN SET UP " << r << endl;

   int cmd = 2;
   secconn.send((char*)&cmd, 4);

   char buf[128];
   strcpy(buf, username.c_str());
   secconn.send(buf, 64);
   strcpy(buf, password.c_str());
   secconn.send(buf, 128);

   int32_t port = m_GMP.getPort();
   secconn.send((char*)&port, 4);
   secconn.recv((char*)&m_iKey, 4);

   cout << "RECV RES " << m_iKey << endl;

   secconn.close();
   SSLTransport::destroy();

   return m_iKey;
}

void Client::logout()
{
   SectorMsg msg;
   msg.setKey(m_iKey);
   msg.setType(2);
   msg.m_iDataLength = SectorMsg::m_iHdrSize;

   m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg);
}

int Client::close()
{
   if (m_iCount -- == 0)
   {
      m_strServerHost = "";
      m_strServerIP = "";
      m_iServerPort = 0;
      m_GMP.close();
   }

   return 1;
}

int Client::list(const string& path, vector<SNode>& attr)
{
   string revised_path = revisePath(path);

   SectorMsg msg;
   msg.resize(65536);
   msg.setType(101);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   cout << "LS " << m_strServerIP << " " <<  m_iServerPort << endl;

   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   string filelist = msg.getData();

   unsigned int s = 0;
   while (s < filelist.length())
   {
      int t = filelist.find(';', s);
      SNode sn;
      sn.deserialize(filelist.substr(s, t - s).c_str());
      attr.insert(attr.end(), sn);
      s = t + 1;
   }

   return attr.size();
}

int Client::stat(const string& path, SNode& attr)
{
   string revised_path = revisePath(path);

   SectorMsg msg;
   msg.resize(65536);
   msg.setType(102);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return -1;

   attr.deserialize(msg.getData());

   int n = (msg.m_iDataLength - SectorMsg::m_iHdrSize - 128) / 68;
   char* al = msg.getData() + 128;

   for (int i = 0; i < n; ++ i)
   {
      Address addr;
      addr.m_strIP = al + 68 * i;
      addr.m_iPort = *(int32_t*)(al + 68 * i + 64);
      attr.m_sLocation.insert(addr);
   }

   return 1;
}

int Client::mkdir(const string& path)
{
   string revised_path = revisePath(path);

   SectorMsg msg;
   msg.setType(103);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return -1;

   return 1;
}

int Client::move(const string& oldpath, const string& newpath)
{
   string revised_path = revisePath(oldpath);

   SectorMsg msg;
   msg.setType(103);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return -1;

   return 1;
}

int Client::remove(const string& path)
{
   string revised_path = revisePath(path);

   SectorMsg msg;
   msg.setType(105);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return -1;

   return 1;
}

string Client::revisePath(const string& path)
{
   if (path.c_str()[0] != '/')
      return "/" + path;

   return path;
}
