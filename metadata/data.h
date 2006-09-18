#ifndef __DATA_H__
#define __DATA_H__

#include <cstdio>
#include <string>
#include <vector>
#include <algorithm>

using namespace std;

struct CAttribute
{
   char m_pcID[256];
   int m_iType;
   int m_iArray;
   int m_iWidth;
};

class CTable
{
public:
   CTable();
   CTable(const CTable& t);
   ~CTable();

public:
   int addAttr(const char* id, int type, int array, int width);

   int serialize(vector<string>& table);
   int deserialize(vector<string>& table);

   void dispaly();

   int size();

public:
   char m_pcName[256];
   vector<CAttribute> m_vAttr;

   int m_iSize;
};

typedef CTable CView;

#endif
