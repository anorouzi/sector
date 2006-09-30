#ifndef __ROUTING_H__
#define __ROUTING_H__

#include <vector>
#include <dhash.h>
#include <gmp.h>

using namespace std;


struct Node
{
   uint32_t m_uiID;
   char m_pcIP[64];
   int32_t m_iPort;
};

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
   int start(const char* ip);
   int join(const char* ip, const char* peer_ip, const int& peer_port);

public:
   int lookup(const unsigned int& key, Node* n);

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
   unsigned int m_uiID;

   int m_iKeySpace;

private:
   vector<FTItem> m_vFingerTable;
   Node m_Successor;
   Node m_Predecessor;
   vector<Node> m_vBackupSuccessors;

private:
   vector<unsigned int> m_vKeys;

private:
   CGMP* m_pGMP;

public:
   static const int m_iRouterPort;
};

#endif
