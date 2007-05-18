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


#include <sqlclient.h>
#include <iostream>

using namespace std;
using namespace cb;

Query* Client::createQueryHandle()
{
   Query *q = NULL;

   try
   {
      q = new Query;
   }
   catch (...)
   {
      return NULL;
   }

   return q;
}

void Client::releaseQueryHandle(Query* q)
{
   delete q;
}

int Client::getSemantics(const string& name, vector<DataAttr>& attr)
{
   CCBMsg msg;
   msg.setType(201); // semantics
   msg.setData(0, name.c_str(), name.length() + 1);
   msg.m_iDataLength = 4 + name.length() + 1;

cout << m_strServerHost.c_str() << " " << m_iServerPort << endl;

   if (m_pGMP->rpc(m_strServerHost.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

cout << "got response\n";

   if (msg.getType() > 0)
      Semantics::deserialize(msg.getData(), attr);

   return msg.getType();
}

Query::Query()
{
   m_GMP.init(0);
}

Query::~Query()
{
   m_GMP.close();
}

int Query::open(const string& query)
{
   if (0 != SQLParser::parse(query, m_SQLExpr))
      return -1;

cout << "parsing .. " << m_SQLExpr.m_vstrFieldList.size() << " " << m_SQLExpr.m_vstrTableList.size() << endl;

   // currently we can only deal with single table
   if (m_SQLExpr.m_vstrTableList.size() != 1)
      return 1;

   string table = m_SQLExpr.m_vstrTableList[0];

   Node n;
   if (Client::lookup(table, &n) < 0)
      return -1;

   CCBMsg msg;
   msg.setType(1); // locate file
   msg.setData(0, table.c_str(), table.length() + 1);
   msg.m_iDataLength = 4 + table.length() + 1;

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
      // SQL client is read only.
      return -1;
   }


   m_uSock = UDT::socket(AF_INET, SOCK_STREAM, 0);

   sockaddr_in my_addr;
   my_addr.sin_family = AF_INET;
   my_addr.sin_port = 0;
   my_addr.sin_addr.s_addr = INADDR_ANY;
   memset(&(my_addr.sin_zero), '\0', 8);
   UDT::bind(m_uSock, (sockaddr*)&my_addr, sizeof(my_addr));
   int size = sizeof(sockaddr_in);
   UDT::getsockname(m_uSock, (sockaddr*)&my_addr, &size);

   msg.setType(200); // submit sql request
   msg.setData(0, (char*)&(my_addr.sin_port), 4);
   msg.setData(4, table.c_str(), table.length() + 1);
   msg.setData(68, query.c_str(), query.length() + 1);
   msg.m_iDataLength = 4 + 4 + 64 + query.length() + 1;
   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   #ifdef WIN32
      int mtu = 1052;
      UDT::setsockopt(m_uSock, 0, UDT_MSS, &mtu, sizeof(int));
   #endif

   int rendezvous = 1;
   UDT::setsockopt(m_uSock, 0, UDT_RENDEZVOUS, &rendezvous, 4);

   sockaddr_in serv_addr;
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_port = *(int*)(msg.getData()); // port
   #ifndef WIN32
      inet_pton(AF_INET, m_strServerIP.c_str(), &serv_addr.sin_addr);
   #else
      serv_addr.sin_addr.s_addr = inet_addr(m_strServerIP.c_str());
   #endif
      memset(&(serv_addr.sin_zero), '\0', 8);

   cout << "connect " << m_strServerIP << " " << *(int*)(msg.getData()) << endl;

   if (UDT::ERROR == UDT::connect(m_uSock, (sockaddr*)&serv_addr, sizeof(serv_addr)))
      return -1;

   m_strQuery = query;

   return 0;
}

int Query::close()
{
   int32_t cmd = 2; // close

   if (UDT::send(m_uSock, (char*)&cmd, 4, 0) < 0)
      return -1;

   UDT::close(m_uSock);

   return 1;
}

int Query::fetch(char* res, int& rows, int& size)
{
   char req[8];
   *(int32_t*)req = 1; // fetch (more) records
   *(int32_t*)(req + 4) = rows;

   if (UDT::send(m_uSock, req, 8, 0) < 0)
      return -1;
   if ((UDT::recv(m_uSock, (char*)&rows, 4, 0) < 0) || (-1 == rows))
      return -1;
   if ((UDT::recv(m_uSock, (char*)&size, 4, 0) < 0) || (-1 == size))
      return -1;

   int h;
   cout << "to recv " << rows << " " << size << endl;
   if (UDT::recv(m_uSock, res, size, 0, &h) < 0)
      return -1;

   return size;
}
