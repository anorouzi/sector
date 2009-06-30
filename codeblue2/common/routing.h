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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/24/2009
*****************************************************************************/


#ifndef __SECTOR_ROUTING_H__
#define __SECTOR_ROUTING_H__

#include <vector>
#include <map>
#include <string>
#include "dhash.h"
#include "topology.h"

class Routing
{
public:
   Routing();
   ~Routing();

public:
   int insert(const uint32_t& key, const Address& node);
   int remove(const uint32_t& key);

   int lookup(const std::string& path, Address& node);
   int lookup(const uint32_t& key, Address& node);

   int getEntityID(const std::string& path);

   int getRouterID(const uint32_t& key);
   int getRouterID(const Address& node);

public:
   std::vector<uint32_t> m_vFingerTable;
   std::map<uint32_t, Address> m_mAddressList;
   std::map<Address, uint32_t, AddrComp> m_mKeyList;

   int m_iKeySpace;
};

#endif
