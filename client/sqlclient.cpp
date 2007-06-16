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

   m_strQuery = query;

   int port;
   m_DataChn.open(port);

   msg.setType(200); // submit sql request
   msg.setData(0, (char*)&port, 4);
   msg.setData(4, table.c_str(), table.length() + 1);
   msg.setData(68, query.c_str(), query.length() + 1);
   msg.m_iDataLength = 4 + 4 + 64 + query.length() + 1;
   if (m_GMP.rpc(m_strServerIP.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   return m_DataChn.connect(m_strServerIP.c_str(), *(int*)(msg.getData()));
}

int Query::close()
{
   int32_t cmd = 2; // close

   m_DataChn.send((char*)&cmd, 4);

   m_DataChn.close();

   return 1;
}

int Query::fetch(char* res, int& rows, int& size)
{
   char req[8];
   *(int32_t*)req = 1; // fetch (more) records
   *(int32_t*)(req + 4) = rows;

   if (m_DataChn.send(req, 8) < 0)
      return -1;
   if ((m_DataChn.recv((char*)&rows, 4) < 0) || (-1 == rows))
      return -1;
   if ((m_DataChn.recv((char*)&size, 4) < 0) || (-1 == size))
      return -1;

   cout << "to recv " << rows << " " << size << endl;
   if (m_DataChn.recv(res, size) < 0)
      return -1;

   return size;
}
