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
   Yunhong Gu [gu@lac.uic.edu], last updated 04/06/2007
*****************************************************************************/


#include "client.h"
using namespace cb;

string Client::m_strServerHost = "";
int Client::m_iServerPort = 0;
CGMP* Client::m_pGMP = NULL;
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

   m_strServerHost = server;
   m_iServerPort = port;

   m_pGMP = new CGMP;
   m_pGMP->init(0);

   return 1;
}

int Client::close()
{
   if (m_iCount -- == 0)
   {
      m_strServerHost = "";
      m_iServerPort = 0;
      m_pGMP->close();
      delete m_pGMP;
   }

   return 1;
}

int Client::lookup(const string& name, Node* n)
{
   CCBMsg msg;
   msg.setType(4); // look up a file server
   msg.setData(0, name.c_str(), name.length() + 1);
   msg.m_iDataLength = 4 + name.length() + 1;

   if (m_pGMP->rpc(m_strServerHost.c_str(), m_iServerPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() > 0)
      memcpy(n, msg.getData(), sizeof(Node));

   return msg.getType();
}

int Client::lookup(const string& name, vector<Node>& nl)
{
   nl.clear();

   Node n;
   if (lookup(name, &n) < 0)
      return 0;

   CCBMsg msg;
   msg.resize(65536);
   msg.setType(1); // locate file
   msg.setData(0, name.c_str(), name.length() + 1);
   msg.m_iDataLength = 4 + name.length() + 1;

   if (m_pGMP->rpc(n.m_pcIP, n.m_iAppPort, &msg, &msg) < 0)
      return 0;

   int num = (msg.m_iDataLength - 4) / 68;
   n.m_uiID = 0;
   n.m_iPort = 0;
   for (int i = 0; i < num; ++ i)
   {
      strcpy(n.m_pcIP, msg.getData() + 68 * i);
      n.m_iAppPort = *(int*)(msg.getData() + 68 * i + 64);
      nl.insert(nl.end(), n);
   }

   return nl.size();
}

int Client::checkServStatus(const vector<Node>& nl, vector<NodeInfo>& il)
{
   il.clear();
   il.resize(nl.size());

   CCBMsg msg;
   msg.setType(1); // query server information
   msg.m_iDataLength = 4;

   vector<NodeInfo>::iterator c = il.begin();
   for (vector<Node>::const_iterator i = nl.begin(); i != nl.end(); ++ i)
   {
      if (m_pGMP->rpc(i->m_pcIP, i->m_iAppPort, &msg, &msg) > 0)
      {
         *c = *(NodeInfo*)msg.getData();
         c->m_iRTT = m_pGMP->rtt(i->m_pcIP, i->m_iAppPort);
      }
      else
         c->m_iStatus = -1;

      ++ c;
   }

   return 1;
}
