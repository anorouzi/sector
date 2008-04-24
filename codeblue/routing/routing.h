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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#ifndef __ROUTING_H__
#define __ROUTING_H__

#include <node.h>
#include <gmp.h>

namespace cb
{

class CRouting
{
public:
   CRouting();
   virtual ~CRouting();

public:
   virtual int start(const char* ip, const int& port) = 0;
   virtual int join(const char* ip, const char* peer_ip, const int& port, const int& peer_port) = 0;

public:
   virtual int lookup(const unsigned int& key, Node* n) = 0;
   virtual bool has(const unsigned int& id) = 0;
   void setAppPort(const int& port);

protected:
   char m_pcIP[64];			// IP address
   int m_iPort;				// port
   uint32_t m_uiID;			// DHash ID

   int m_iAppPort;			// Application port

protected:
   int m_iRouterPort;			// default router port

protected:
   int m_iKeySpace;                     // DHash key space
   uint32_t hash(const char* ip, const int& port);

protected:
   CGMP* m_pGMP;                        // GMP messenger
};

}; // namespace

#endif
