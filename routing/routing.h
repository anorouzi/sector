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


#ifndef __ROUTING_H__
#define __ROUTING_H__

#include <vector>
#include <dhash.h>
#include <gmp.h>
#include <node.h>

using namespace std;

namespace cb
{

struct FTItem
{
   unsigned int m_uiStart;
   Node m_Node;
};

struct KeyItem
{
   unsigned int m_uiKey;
   char m_pcName[64];
   char m_pcIP[64];
   int m_iPort;
};

class CRouting
{
public:
   CRouting();
   ~CRouting();

public:
   int start(const char* ip, const int& port = 0);
   int join(const char* ip, const char* peer_ip, const int& port = 0, const int& peer_port = 0);

public:
   int lookup(const unsigned int& key, Node* n);
   void setAppPort(const int& port);
   bool has(const unsigned int& id);

private:
   int find_successor(const unsigned int& id, Node* n);
   void closest_preceding_finger(const unsigned int& id, Node* n);

private:
   void init_finger_table();
   void print_finger_table();

   void stabilize();
   void notify(Node* n);
   void fix_fingers(int& next);
   void check_predecessor();
   void check_successor(int& next);

   uint32_t hash(const char* ip, const int& port);

private:
   struct Param
   {
      CRouting* r;
      char ip[64];
      int port;
      int32_t id;
      CRTMsg* msg;
   };

   static void* run(void* r);
   static void* process(void* p);
   static void* stabilize(void* r);

private:
   char m_pcIP[64];
   int m_iPort;
   uint32_t m_uiID;

   int m_iAppPort;

   int m_iKeySpace;

private:
   vector<FTItem> m_vFingerTable;
   Node m_Successor;
   Node m_Predecessor;
   vector<Node> m_vBackupSuccessors;

private:
   CGMP* m_pGMP;

public:
   static const int m_iRouterPort;
};

}; // namespace

#endif
