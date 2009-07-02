/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/21/2009
*****************************************************************************/

#include "routing.h"

using namespace std;

Routing::Routing():
m_iKeySpace(32)
{
}

Routing::~Routing()
{
}

void Routing::init()
{
   m_vFingerTable.clear();
   m_mAddressList.clear();
   m_mKeyList.clear();
}

int Routing::insert(const uint32_t& key, const Address& node)
{
   if (m_mAddressList.find(key) != m_mAddressList.end())
      return -1;

   m_mAddressList[key] = node;
   m_mKeyList[node] = key;

   bool found = false;
   for (vector<uint32_t>::iterator i = m_vFingerTable.begin(); i != m_vFingerTable.end(); ++ i)
   {
      if (key > *i)
      {
         m_vFingerTable.insert(i, key);
         found = true;
         break;
      }
   }
   if (!found)
      m_vFingerTable.insert(m_vFingerTable.end(), key);

   return 1;
}

int Routing::remove(const uint32_t& key)
{
   map<uint32_t, Address>::iterator k = m_mAddressList.find(key);
   if (k == m_mAddressList.end())
      return -1;

   m_mKeyList.erase(k->second);
   m_mAddressList.erase(k);

   for (vector<uint32_t>::iterator i = m_vFingerTable.begin(); i != m_vFingerTable.end(); ++ i)
   {
      if (key == *i)
      {
         m_vFingerTable.erase(i);
         break;
      }
   }

   return 1;
}

int Routing::lookup(const uint32_t& key, Address& node)
{
   if (m_vFingerTable.empty())
      return -1;

   int f = key % m_vFingerTable.size();
   int r = m_vFingerTable[f];
   node = m_mAddressList[r];

   return 1;
}

int Routing::lookup(const string& path, Address& node)
{
   uint32_t key = DHash::hash(path.c_str(), m_iKeySpace);

   return lookup(key, node);
}

int Routing::getEntityID(const string& path)
{
   uint32_t key = DHash::hash(path.c_str(), m_iKeySpace);

   if (m_vFingerTable.empty())
      return -1;

   return key % m_vFingerTable.size();
}

int Routing::getRouterID(const uint32_t& key)
{
   int pos = 0;
   for (vector<uint32_t>::const_iterator i = m_vFingerTable.begin(); i != m_vFingerTable.end(); ++ i)
   {
      if (*i == key)
         return pos;
      ++ pos;
   }
   return -1;
}

int Routing::getRouterID(const Address& node)
{
   map<Address, uint32_t, AddrComp>::iterator a = m_mKeyList.find(node);
   if (a == m_mKeyList.end())
      return -1;

   return getRouterID(a->second);
}

bool Routing::match(const uint32_t& cid, const uint32_t& key)
{
   if (m_vFingerTable.empty())
      return false;

   return key == m_vFingerTable[cid % m_vFingerTable.size()];
}

bool Routing::match(const char* path, const uint32_t& key)
{
   if (m_vFingerTable.empty())
      return false;

   uint32_t pid = DHash::hash(path, m_iKeySpace);
   return key == m_vFingerTable[pid % m_vFingerTable.size()];
}
