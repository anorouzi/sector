#include "data.h"
#include <iostream>

using namespace std;

CTable::CTable():
m_iSize(0)
{
   m_vAttr.clear();
}

CTable::CTable(const CTable& t):
m_iSize(t.m_iSize)
{
   strcpy(m_pcName, t.m_pcName);
   copy(t.m_vAttr.begin(), t.m_vAttr.end(), m_vAttr.begin());
}

CTable::~CTable()
{
   m_vAttr.clear();
}

int CTable::addAttr(const char* id, int type, int array, int width)
{
   CAttribute attr;

   if (strlen(id) > 256)
      return -1;

   memcpy(attr.m_pcID, id, strlen(id));
   attr.m_iType = type;
   attr.m_iArray = array;
   attr.m_iWidth = width;

   m_vAttr.insert(m_vAttr.end(), attr);

   if (0 == array)
      m_iSize += width;
   else
      m_iSize += array * width;

   return 1;
}

int CTable::serialize(vector<string>& table)
{
   table.clear();

   char str[512];

   for (vector<CAttribute>::iterator i = m_vAttr.begin(); i != m_vAttr.end(); ++ i)
   {
      sprintf(str, "%s %d %d %d", i->m_pcID, i->m_iType, i->m_iArray, i->m_iWidth);
      table.insert(table.end(), str);
   }

   return table.size();
}

int CTable::deserialize(vector<string>& table)
{
   m_vAttr.clear();
   m_iSize = 0;

   for (vector<string>::iterator i = table.begin(); i != table.end(); i ++)
   {
      const char* p = i->c_str();
      CAttribute attr;

      int c = 0;
      while (' ' == *(p + c))
         ++ c;

      memcpy(attr.m_pcID, p, c);
      attr.m_pcID[c] = '\0';

      p += c;
      char* str = (char*)p;

      attr.m_iType = strtol(p, &str, 10);
      p = str;
      attr.m_iArray = strtol(p, &str, 10);
      p = str;
      attr.m_iWidth = strtol(p, &str, 10);

      m_vAttr.insert(m_vAttr.end(), attr);

      if (0 == attr.m_iArray)
         m_iSize += attr.m_iWidth;
      else
         m_iSize += attr.m_iArray * attr.m_iWidth;
   }

   return m_vAttr.size();
}

void CTable::dispaly()
{
   for (vector<CAttribute>::iterator i = m_vAttr.begin(); i != m_vAttr.end(); i ++)
   {
      cout << i->m_pcID << " ";
      cout << i->m_iType;
      if (i->m_iArray > 1)
         cout << "[" << i->m_iArray << "]";
      cout << endl;
   }
}

int CTable::size()
{
   return m_iSize;
}
