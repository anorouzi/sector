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
}

CRouting::~CRouting()
{
   m_pGMP->close();

   delete m_pGMP;
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

   CRTMsg msg;
   msg.setType(3); // find_successor
   msg.setData(0, (char*)&(m_vFingerTable[0].m_uiStart), 4);
   msg.m_iDataLength = 4 + 4;

   if (m_pGMP->rpc(n.m_pcIP, n.m_iPort, &msg, &msg) < 0)
      return -1;

   m_Successor = m_vFingerTable[0].m_Node = *(Node*)(msg.getData());

   pthread_t stabilizer;
   pthread_create(&stabilizer, NULL, stabilize, this);
   pthread_detach(stabilizer);

   return 0;
}

void CRouting::setAppPort(const int& port)
{
   m_iAppPort = port;
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

   if (((m_uiID < m_Successor.m_uiID) && (m_uiID < id) && (id <= m_Successor.m_uiID)) ||
       ((m_uiID >= m_Successor.m_uiID) && ((m_uiID < id) || (id <= m_Successor.m_uiID))))
   {
      *n = m_Successor;

      return 0;
   }


   Node c;
   closest_preceding_finger(id, &c);

   CRTMsg msg;
   msg.setType(3); // find successor
   msg.setData(0, (char*)&id, 4);
   msg.m_iDataLength = 4 + 4;

   int res = m_pGMP->rpc(c.m_pcIP, c.m_iPort, &msg, &msg);

   if (res >= 0)
      *n = *(Node*)(msg.getData());

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

void CRouting::init_finger_table()
{
   for (int i = 0; i < m_iKeySpace; ++ i)
   {
      FTItem f;

      f.m_uiStart = (m_uiID + int(pow(2.0, double(i)))) % int(pow(2.0, double(m_iKeySpace)));
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
}

void CRouting::print_finger_table()
{
   cout << "----------------- " << m_uiID << " -----------------\n";
   for (int i = 0; i < m_iKeySpace; ++ i)
   {
      cout << m_vFingerTable[i].m_uiStart << " " << m_vFingerTable[i].m_Node.m_uiID << " " << m_vFingerTable[i].m_Node.m_pcIP << endl;
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
         m_Successor = m_vFingerTable[0].m_Node = *(Node*)(msg.getData());
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
   if (m_Predecessor.m_iPort == 0)
   {
      m_Predecessor = *n;
      return;
   }

   if (((m_Predecessor.m_uiID < m_uiID) && (m_Predecessor.m_uiID < n->m_uiID) && (n->m_uiID < m_uiID)) ||
        ((m_Predecessor.m_uiID >= m_uiID) && ((m_Predecessor.m_uiID < n->m_uiID) || (n->m_uiID < m_uiID))))
   {
      m_Predecessor = *n;
   }
}

void CRouting::fix_fingers(int& next)
{
   if (find_successor(m_vFingerTable[next].m_uiStart, &(m_vFingerTable[next].m_Node)) >= 0)
   {
      if (0 == next)
         m_Successor = m_vFingerTable[0].m_Node;
   }

   ++ next;
   if (next == m_iKeySpace)
      next = 0;
}

void CRouting::check_predecessor()
{
   // if there is no predecessor, return;
   if (m_Predecessor.m_iPort == 0)
      return;

   CRTMsg msg;

   msg.setType(6); // check predecessor
   msg.m_iDataLength = 4;

   int res = m_pGMP->rpc(m_Predecessor.m_pcIP, m_Predecessor.m_iPort, &msg, &msg);

   if ((res < 0) || (msg.getType() < 0))
   {
      m_Predecessor.m_pcIP[0] = '\0';
      m_Predecessor.m_iPort = 0;
   }
}

void CRouting::check_successor(int& next)
{
   CRTMsg msg;

   msg.setType(1); // get successor
   msg.m_iDataLength = 4;

   char* ip;
   int port;

   if (0 == next)
   {
      ip = m_Successor.m_pcIP;
      port = m_Successor.m_iPort;
   }
   else
   {
      ip = m_vBackupSuccessors[next - 1].m_pcIP;
      port = m_vBackupSuccessors[next - 1].m_iPort;
   }

   int res = m_pGMP->rpc(ip, port, &msg, &msg);

   if ((res < 0) || (msg.getType() < 0))
   {
      if (0 == next)
      {
         if (m_vBackupSuccessors.size() > 0)
         {
            m_Successor = m_vFingerTable[0].m_Node = m_vBackupSuccessors[0];
            m_vBackupSuccessors.erase(m_vBackupSuccessors.begin());
         }
         else
         {
            // no successor found, isolated
            memcpy(m_Successor.m_pcIP, m_pcIP, 64);
            m_Successor.m_iPort = m_iPort;
            m_Successor.m_iAppPort = m_iAppPort;
            m_Successor.m_uiID = m_uiID;
            m_vFingerTable[0].m_Node = m_Successor;
         }
      }
      else
      {
         // bad node, remove it
         m_vBackupSuccessors.erase(m_vBackupSuccessors.begin() + next - 1);

         next --;
         if (next < 0)
            next = 0;
      }
   }
   else
   {
      if (m_uiID == ((Node*)(msg.getData()))->m_uiID)
      {
         // loop back, remove all additional (already gone) successors
         if (next < int(m_vBackupSuccessors.size()))
            m_vBackupSuccessors.erase(m_vBackupSuccessors.begin() + next, m_vBackupSuccessors.end());

          next = 0;
      }
      else
      {
         // everything is OK, update next successor
         if (next < int(m_vBackupSuccessors.size()))
            m_vBackupSuccessors[next] = *(Node*)(msg.getData());
         else
            m_vBackupSuccessors.insert(m_vBackupSuccessors.end(), *(Node*)(msg.getData()));

         next = (next + 1) % m_iKeySpace;
      }
   }
}

void* CRouting::run(void* r)
{
   CRouting* self = (CRouting*)r;

   char ip[64];
   int port;
   int32_t id;
   CRTMsg* msg;

   while (true)
   {
      msg = new CRTMsg;

      self->m_pGMP->recvfrom(ip, port, id, msg);

      Param* p = new Param;
      p->r = self;
      memcpy(p->ip, ip, 64);
      p->id = id;
      p->port = port;
      p->msg = msg;

      pthread_t process_thread;
      pthread_create(&process_thread, NULL, process, p);
      pthread_detach(process_thread);
   }
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
   case 1: // get Successor
      msg->setData(0, (char*)&self->m_Successor, sizeof(Node));
      msg->m_iDataLength = 4 + sizeof(Node);

      break;

   case 2: // get Predecessor
      msg->setData(0, (char*)&self->m_Predecessor, sizeof(Node));
      msg->m_iDataLength = 4 + sizeof(Node);

      break;

   case 3: // find successor
      {
      int id = *(int*)(msg->getData());

      if (self->find_successor(id, (Node*)(msg->getData())) < 0)
         msg->setType(-msg->getType());

      msg->m_iDataLength = 4 + sizeof(Node);

      break;
      }

   case 4: // notify
      self->notify((Node*)msg->getData());

      msg->m_iDataLength = 4;

      break;

   case 5: // closest_preceding_node
      {
      int id = *(int*)(msg->getData());
      self->closest_preceding_finger(id, (Node*)(msg->getData()));

      msg->m_iDataLength = 4 + sizeof(Node);

      break;
      }

   case 6: // check status
      msg->m_iDataLength = 4;
      break;

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

   int nextf = 0;
   int nexts = 0;

   while (true)
   {
      sleep(10);
      //cout << "stabilizing...\n";
      self->stabilize();
      //cout << "stabilized\n";

      sleep(10);
      //cout << "fixing fingers " << nextf << endl;
      self->fix_fingers(nextf);
      //cout << "fixed fingers " << nextf << endl;

      sleep(10);
      //cout << "checking predecessors " << endl;
      self->check_predecessor();
      //cout << "checked predecessors " << endl;

      sleep(10);
      //cout << "checking successor " << nexts << endl;
      self->check_successor(nexts);
      //cout << "checked successor " << nexts << endl;

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
