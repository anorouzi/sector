/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

SECTOR: A Distributed Storage and Computing Infrastructure

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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/01/2007
*****************************************************************************/


#include <routing.h>
#include <iostream>

using namespace std;
using namespace cb;

const int CRouting::m_iRouterPort = 24673;      //chord

CRouting::CRouting():
m_iAppPort(0),
m_iKeySpace(32)
{
   m_pGMP = new CGMP;

   m_vFingerTable.clear();
   m_vFingerTable.resize(m_iKeySpace);

   m_vBackupSuccessors.clear();

   pthread_mutex_init(&m_PKeyLock, NULL);
   pthread_mutex_init(&m_SKeyLock, NULL);
}

CRouting::~CRouting()
{
   m_pGMP->close();
   delete m_pGMP;

   pthread_mutex_destroy(&m_PKeyLock);
   pthread_mutex_destroy(&m_SKeyLock);
}

int CRouting::start(const char* ip, const int& port)
{
   strcpy(m_pcIP, ip);
   if (port > 0)
      m_iPort = port;
   else
      m_iPort = m_iRouterPort;

   m_uiID = hash(m_pcIP, m_iPort);

   m_pGMP->init(m_iPort);

   init_finger_table();

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, run, this);
   pthread_detach(msgserver);

   pthread_t stabilizer;
   pthread_create(&stabilizer, NULL, stabilize, this);
   pthread_detach(stabilizer);

   return 0;
}

int CRouting::join(const char* ip, const char* peer_ip, const int& port, const int& peer_port)
{
   strcpy(m_pcIP, ip);
   if (port > 0)
      m_iPort = port;
   else
      m_iPort = m_iRouterPort;

   m_uiID = hash(m_pcIP, m_iPort);

   m_pGMP->init(m_iPort);

   init_finger_table();

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, run, this);
   pthread_detach(msgserver);

   Node n;
   strcpy(n.m_pcIP, peer_ip);
   if (peer_port > 0)
      n.m_iPort = peer_port;
   else
      n.m_iPort = m_iRouterPort;
   n.m_uiID = hash(n.m_pcIP, n.m_iPort);

   init_finger_table(&n);

   pthread_t stabilizer;
   pthread_create(&stabilizer, NULL, stabilize, this);
   pthread_detach(stabilizer);

   return 0;
}

void CRouting::setAppPort(const int& port)
{
   m_iAppPort = port;
}

bool CRouting::has(const unsigned int& id)
{
   char pred_port;
   uint32_t pred_id;

   pthread_mutex_lock(&m_PKeyLock);
   pred_port = m_Predecessor.m_iPort;
   pred_id = m_Predecessor.m_uiID;
   pthread_mutex_unlock(&m_PKeyLock);

   if (0 == pred_port)
      return true;

   if (pred_id < m_uiID)
      return ((pred_id < id) && (id <= m_uiID));

   if (pred_id > m_uiID)
      return ((pred_id < id) || (id <= m_uiID));

   return (id == m_uiID);
}

int CRouting::lookup(const unsigned int& key, Node* n)
{
   return find_successor(key, n);
}

int CRouting::find_successor(const unsigned int& id, Node* n)
{
   if (id == m_uiID)
   {
      n->m_uiID = m_uiID;
      memcpy(n->m_pcIP, m_pcIP, 64);
      n->m_iPort = m_iPort;
      n->m_iAppPort = m_iAppPort;
      return 0;
   }

   pthread_mutex_lock(&m_SKeyLock);

   if (((m_uiID < m_Successor.m_uiID) && (m_uiID < id) && (id <= m_Successor.m_uiID)) ||
       ((m_uiID >= m_Successor.m_uiID) && ((m_uiID < id) || (id <= m_Successor.m_uiID))))
   {
      *n = m_Successor;
      pthread_mutex_unlock(&m_SKeyLock);
      return 0;
   }

   Node c;
   closest_preceding_finger(id, &c);

   pthread_mutex_unlock(&m_SKeyLock);

   CRTMsg msg;
   msg.setType(3); // find successor
   msg.setData(0, (char*)&id, 4);
   msg.m_iDataLength = 4 + 4;

   int res = m_pGMP->rpc(c.m_pcIP, c.m_iPort, &msg, &msg);

   if (res >= 0)
   {
      pthread_mutex_lock(&m_SKeyLock);
      *n = *(Node*)(msg.getData());
      pthread_mutex_unlock(&m_SKeyLock);
   }

   return res;
}

void CRouting::closest_preceding_finger(const unsigned int& id, Node* n)
{
   for (int i = m_iKeySpace - 1; i >= 0; -- i)
   {
      if (((m_uiID < id) && (m_vFingerTable[i].m_Node.m_uiID > m_uiID) && (m_vFingerTable[i].m_Node.m_uiID < id)) || 
          ((m_uiID >= id) && ((m_vFingerTable[i].m_Node.m_uiID > m_uiID) || (m_vFingerTable[i].m_Node.m_uiID < id))))
         {
            *n = m_vFingerTable[i].m_Node;
            return;
         }
   }

   n->m_uiID = m_uiID;
   memcpy(n->m_pcIP, m_pcIP, 64);
   n->m_iPort = m_iPort;
   n->m_iAppPort = m_iAppPort;
}

void CRouting::init_finger_table(const Node* n)
{
   // a standing along node
   for (int i = 0; i < m_iKeySpace; ++ i)
   {
      FTItem f;

      f.m_uiStart = (uint32_t)(((m_uiID + (1LL << i)) % (1LL << m_iKeySpace)));
      f.m_Node.m_uiID = m_uiID;
      memcpy(f.m_Node.m_pcIP, m_pcIP, 64);
      f.m_Node.m_iPort = m_iPort;
      f.m_Node.m_iAppPort = m_iAppPort;

      m_vFingerTable[i] = f;
   }

   m_Successor = m_vFingerTable[0].m_Node;

   // NULL predecessor
   m_Predecessor.m_pcIP[0] = '\0';
   m_Predecessor.m_iPort = 0;

   if (NULL == n)
      return;

   // joining the existing network
   for (int i = 0; i < m_iKeySpace; ++ i)
   {
      m_vFingerTable[i].m_uiStart = (uint32_t)(((m_uiID + (1LL << i)) % (1LL << m_iKeySpace)));

      CRTMsg msg;
      msg.setType(3); // find_successor
      msg.setData(0, (char*)&(m_vFingerTable[i].m_uiStart), 4);
      msg.m_iDataLength = 4 + 4;

      if (m_pGMP->rpc(n->m_pcIP, n->m_iPort, &msg, &msg) < 0)
         break;

      m_vFingerTable[i].m_Node = *(Node*)(msg.getData());
   }

   m_Successor = m_vFingerTable[0].m_Node;

   CRTMsg msg;
   msg.setType(2); // get predecessor
   msg.m_iDataLength = 4;

   if (m_pGMP->rpc(m_Successor.m_pcIP, m_Successor.m_iPort, &msg, &msg) > 0)
      m_Predecessor = *(Node*)(msg.getData());
}

void CRouting::print_finger_table()
{
   cout << "----------------- " << m_uiID << " -----------------\n";
   for (int i = 0; i < m_iKeySpace; ++ i)
   {
      cout << m_vFingerTable[i].m_uiStart << " " << m_vFingerTable[i].m_Node.m_uiID << " " << m_vFingerTable[i].m_Node.m_pcIP << ":" << m_vFingerTable[i].m_Node.m_iPort << endl;
   }
   cout << endl;
   cout << m_Successor.m_uiID << " " << m_Predecessor.m_uiID << endl;
   for (vector<Node>::iterator i = m_vBackupSuccessors.begin(); i != m_vBackupSuccessors.end(); ++ i)
      cout << i->m_uiID << " ";
   cout << endl;
   cout << "--------------------------------------\n";
}

void CRouting::stabilize()
{
   CRTMsg msg;

   msg.setType(2); // get predecessor
   msg.m_iDataLength = 4;

   if (m_pGMP->rpc(m_Successor.m_pcIP, m_Successor.m_iPort, &msg, &msg) < 0)
      return;

   if (((Node*)(msg.getData()))->m_iPort != 0)
   {
      unsigned int pred = ((Node*)(msg.getData()))->m_uiID;

      if (((m_uiID < m_Successor.m_uiID) && (m_uiID < pred) && (pred < m_Successor.m_uiID)) ||
          ((m_uiID >= m_Successor.m_uiID) && ((m_uiID < pred) || (pred < m_Successor.m_uiID))))
      {
         pthread_mutex_lock(&m_SKeyLock);
         m_Successor = m_vFingerTable[0].m_Node = *(Node*)(msg.getData());
         pthread_mutex_unlock(&m_SKeyLock);
      }
   }

   if (m_Successor.m_uiID != m_uiID)
   {
      msg.setType(4); // notify
      ((Node*)(msg.getData()))->m_uiID = m_uiID;
      memcpy(((Node*)(msg.getData()))->m_pcIP, m_pcIP, 64);
      ((Node*)(msg.getData()))->m_iPort = m_iPort;
      ((Node*)(msg.getData()))->m_iAppPort = m_iAppPort;
      msg.m_iDataLength = 4 + sizeof(Node);

      m_pGMP->rpc(m_Successor.m_pcIP, m_Successor.m_iPort, &msg, &msg);
   }
}

void CRouting::notify(Node* n)
{
   pthread_mutex_lock(&m_PKeyLock);

   if ((m_Predecessor.m_iPort == 0) ||
       ((m_Predecessor.m_uiID < m_uiID) && (m_Predecessor.m_uiID < n->m_uiID) && (n->m_uiID < m_uiID)) ||
       ((m_Predecessor.m_uiID >= m_uiID) && ((m_Predecessor.m_uiID < n->m_uiID) || (n->m_uiID < m_uiID))))
   {
      m_Predecessor = *n;
   }

   pthread_mutex_unlock(&m_PKeyLock);
}

void CRouting::fix_fingers()
{
   for (int i = 1; i < m_iKeySpace; ++ i)
      find_successor(m_vFingerTable[i].m_uiStart, &(m_vFingerTable[i].m_Node));
}

void CRouting::check_predecessor()
{
   char pred_ip[64];
   int pred_port;

   pthread_mutex_lock(&m_PKeyLock);
   strcpy(pred_ip, m_Predecessor.m_pcIP);
   pred_port = m_Predecessor.m_iPort;
   pthread_mutex_unlock(&m_PKeyLock);

   // if there is no predecessor, return;
   if (0 == pred_port)
      return;

   CRTMsg msg;

   msg.setType(6); // check predecessor
   msg.m_iDataLength = 4;

   int res = m_pGMP->rpc(pred_ip, pred_port, &msg, &msg);

   if ((res < 0) || (msg.getType() < 0))
   {
      pthread_mutex_lock(&m_PKeyLock);
      m_Predecessor.m_pcIP[0] = '\0';
      m_Predecessor.m_iPort = 0;
      pthread_mutex_unlock(&m_PKeyLock);
   }
}

void CRouting::check_successor()
{
   char ip[64];
   strcpy(ip, m_Successor.m_pcIP);
   int port = m_Successor.m_iPort;

   for (int i = 0; i < m_iKeySpace;)
   {
      CRTMsg msg;
      msg.setType(1); // get successor
      msg.m_iDataLength = 4;
      int res = m_pGMP->rpc(ip, port, &msg, &msg);

      if ((res < 0) || (msg.getType() < 0))
      {
         if (0 == i)
         {
            // successor is lost, get the first from the backup list
            if (m_vBackupSuccessors.size() > 0)
            {
               pthread_mutex_lock(&m_SKeyLock);
               m_Successor = m_vFingerTable[0].m_Node = m_vBackupSuccessors[0];
               m_vBackupSuccessors.erase(m_vBackupSuccessors.begin());
               pthread_mutex_unlock(&m_SKeyLock);

               strcpy(ip, m_Successor.m_pcIP);
               port = m_Successor.m_iPort;
            }
            else
            {
               // no successor found, isolated
               pthread_mutex_lock(&m_SKeyLock);
               memcpy(m_Successor.m_pcIP, m_pcIP, 64);
               m_Successor.m_iPort = m_iPort;
               m_Successor.m_iAppPort = m_iAppPort;
               m_Successor.m_uiID = m_uiID;
               m_vFingerTable[0].m_Node = m_Successor;
               pthread_mutex_unlock(&m_SKeyLock);
               break;
            }
         }
         else
         {
            // bad node, remove it
            pthread_mutex_lock(&m_SKeyLock);
            m_vBackupSuccessors.erase(m_vBackupSuccessors.begin() + i - 1);
            pthread_mutex_unlock(&m_SKeyLock);
            break;
         }
      }
      else
      {
         if (m_uiID == ((Node*)(msg.getData()))->m_uiID)
         {
            // loop back, remove all additional (already gone) successors
            pthread_mutex_lock(&m_SKeyLock);
            m_vBackupSuccessors.erase(m_vBackupSuccessors.begin() + i, m_vBackupSuccessors.end());
            pthread_mutex_unlock(&m_SKeyLock);
            break;
         }
         else
         {
            // everything is OK, update next successor
            pthread_mutex_lock(&m_SKeyLock);
            if (i < int(m_vBackupSuccessors.size()))
            {
               if (m_vBackupSuccessors[i].m_uiID != ((Node*)(msg.getData()))->m_uiID)
                  m_vBackupSuccessors[i] = *(Node*)(msg.getData());
            }
            else
               m_vBackupSuccessors.insert(m_vBackupSuccessors.end(), *(Node*)(msg.getData()));
            pthread_mutex_unlock(&m_SKeyLock);

            strcpy(ip, ((Node*)(msg.getData()))->m_pcIP);
            port = ((Node*)(msg.getData()))->m_iPort;
            ++ i;
         }
      }
   }
}

void* CRouting::run(void* r)
{
   CRouting* self = (CRouting*)r;

   char ip[64];
   int port;
   int32_t id;
   CRTMsg* msg = new CRTMsg;

   while (true)
   {
      self->m_pGMP->recvfrom(ip, port, id, msg);

      //cout << "recv request RT " << msg->getType() << endl;

      switch(msg->getType())
      {
      case 1: // get Successor
         pthread_mutex_lock(&self->m_SKeyLock);
         msg->setData(0, (char*)&self->m_Successor, sizeof(Node));
         pthread_mutex_unlock(&self->m_SKeyLock);
         msg->m_iDataLength = 4 + sizeof(Node);
         self->m_pGMP->sendto(ip, port, id, msg);
         break;

      case 2: // get Predecessor
         pthread_mutex_lock(&self->m_PKeyLock);
         msg->setData(0, (char*)&self->m_Predecessor, sizeof(Node));
         pthread_mutex_unlock(&self->m_PKeyLock);
         msg->m_iDataLength = 4 + sizeof(Node);
         self->m_pGMP->sendto(ip, port, id, msg);
         break;

      case 4: // notify
         self->notify((Node*)msg->getData());
         msg->m_iDataLength = 4;
         self->m_pGMP->sendto(ip, port, id, msg);
         break;

      case 5: // closest_preceding_node
         {
         int id = *(int*)(msg->getData());
         pthread_mutex_lock(&self->m_SKeyLock);
         self->closest_preceding_finger(id, (Node*)(msg->getData()));
         pthread_mutex_unlock(&self->m_SKeyLock);
         msg->m_iDataLength = 4 + sizeof(Node);
         self->m_pGMP->sendto(ip, port, id, msg);
         break;
         }

      case 6: // check status
         msg->m_iDataLength = 4;
         self->m_pGMP->sendto(ip, port, id, msg);
         break;

      default:
         Param* p = new Param;
         p->r = self;
         memcpy(p->ip, ip, 64);
         p->id = id;
         p->port = port;
         p->msg = new CRTMsg(*msg);

         pthread_t process_thread;
         pthread_create(&process_thread, NULL, process, p);
         pthread_detach(process_thread);
      }
   }

   delete msg;
}

void* CRouting::process(void* p)
{
   CRouting* self = ((Param*)p)->r;
   char* ip = ((Param*)p)->ip;
   int port = ((Param*)p)->port;
   int32_t id = ((Param*)p)->id;
   CRTMsg* msg = ((Param*)p)->msg;

   //cout << "recv request RT " << msg->getType() << endl;

   switch (msg->getType())
   {
   case 3: // find successor
      {
      int id = *(int*)(msg->getData());

      if (self->find_successor(id, (Node*)(msg->getData())) < 0)
         msg->setType(-msg->getType());

      msg->m_iDataLength = 4 + sizeof(Node);

      break;
      }

   default:
      break;
   }

   self->m_pGMP->sendto(ip, port, id, msg);

   //cout << "responded RT " << msg->getType() << " " << id << " " << msg->m_iDataLength << endl;

   delete msg;
   delete (Param*)p;

   return NULL;
}

void* CRouting::stabilize(void* r)
{
   CRouting* self = (CRouting*)r;

   while (true)
   {
      sleep(1);
      self->stabilize();

      sleep(1);
      self->fix_fingers();

      sleep(1);
      self->check_predecessor();

      sleep(1);
      self->check_successor();

      //self->print_finger_table();
   }

   return NULL;
}

uint32_t CRouting::hash(const char* ip, const int& port)
{
   char str[64];
   sprintf(str, "%s:%d", ip, port);
   return DHash::hash(str, m_iKeySpace);
}
