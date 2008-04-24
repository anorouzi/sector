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


#ifndef __CHORD_H__
#define __CHORD_H__

#include <vector>
#include <routing.h>

using namespace std;

namespace cb
{

struct FTItem
{
   unsigned int m_uiStart;
   Node m_Node;
};

class Chord: public CRouting
{
public:
   Chord();
   virtual ~Chord();

public:
   int start(const char* ip, const int& port = 0);
   int join(const char* ip, const char* peer_ip, const int& port = 0, const int& peer_port = 0);

public:
   int lookup(const unsigned int& key, Node* n);
   bool has(const unsigned int& id);

private:
   int find_successor(const unsigned int& id, Node* n);
   void closest_preceding_finger(const unsigned int& id, Node* n);

private:
   void init_finger_table(const Node* n = NULL);
   void print_finger_table();

   void stabilize();
   void notify(Node* n);
   void fix_fingers();
   void check_predecessor();
   void check_successor();

private:
   struct Param
   {
      Chord* r;
      char ip[64];
      int port;
      int32_t id;
      CRTMsg* msg;
   };

   static void* process(void* r);
   static void* processEx(void* p);
   static void* stabilize(void* r);

private:
   vector<FTItem> m_vFingerTable;	// route table
   Node m_Successor;			// successor
   Node m_Predecessor;			// predecessor
   vector<Node> m_vBackupSuccessors;	// backup successor

private:
   pthread_mutex_t m_PKeyLock;		// synchronize predecessor access
   pthread_mutex_t m_SKeyLock;		// synchronize successor access
};

}; // namespace

#endif
