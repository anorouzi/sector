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
   Yunhong Gu [gu@lac.uic.edu], last updated 08/16/2007
*****************************************************************************/


#include <center.h>
#include <dhash.h>

using namespace cb;

Center::Center()
{
   m_iKeySpace = 32;
   m_iRouterPort = 23683;      //center

   m_pGMP = new CGMP;
}

Center::~Center()
{
   m_pGMP->close();
   delete m_pGMP;
}

int Center::start(const char* ip, const int& port)
{
   strcpy(m_pcIP, ip);
   if (port > 0)
      m_iPort = port;
   else
      m_iPort = m_iRouterPort;
   m_uiID = hash(m_pcIP, m_iPort);

   m_pGMP->init(m_iPort);

   strcpy(m_Center.m_pcIP, m_pcIP);
   m_Center.m_iPort = m_iPort;
   m_Center.m_uiID = m_uiID;
   m_Center.m_iAppPort = m_iAppPort;

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, process, this);
   pthread_detach(msgserver);

   return 0;
}

int Center::join(const char* ip, const char* peer_ip, const int& port, const int& peer_port)
{
   strcpy(m_pcIP, ip);
   if (port > 0)
      m_iPort = port;
   else
      m_iPort = m_iRouterPort;
   m_uiID = hash(m_pcIP, m_iPort);

   m_pGMP->init(m_iPort);

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, process, this);
   pthread_detach(msgserver);

   strcpy(m_Center.m_pcIP, peer_ip);
   if (peer_port > 0)
      m_Center.m_iPort = peer_port;
   else
      m_Center.m_iPort = m_iRouterPort;
   m_Center.m_uiID = hash(m_Center.m_pcIP, m_Center.m_iPort);

   CRTMsg msg;
   msg.setType(1);
   msg.m_iDataLength = 4;
   if (m_pGMP->rpc(m_Center.m_pcIP, m_Center.m_iPort, &msg, &msg) < 0)
      return -1;

   m_Center.m_iAppPort = *(int*)msg.getData();

   return 0;
}

bool Center::has(const unsigned int& id)
{
   return (m_Center.m_uiID == m_uiID);
}

int Center::lookup(const unsigned int& key, Node* n)
{
   strcpy(n->m_pcIP, m_Center.m_pcIP);
   n->m_iPort = m_Center.m_iPort;
   n->m_uiID = m_Center.m_uiID;
   n->m_iAppPort = m_Center.m_iAppPort;

   return 1;
}

void* Center::process(void* r)
{
   Center* self = (Center*)r;

   char ip[64];
   int port;
   int32_t id;
   CRTMsg* msg = new CRTMsg;

   while (true)
   {
      self->m_pGMP->recvfrom(ip, port, id, msg);

      switch(msg->getType())
      {
      case 1: // get app port
         *(int*)msg->getData() = self->m_iAppPort;
         msg->m_iDataLength = 4 + 4;
         self->m_pGMP->sendto(ip, port, id, msg);
         break;

      default:
         break;
      }
   }

   delete msg;
}
