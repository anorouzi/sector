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


#ifndef __CENTER_H__
#define __CENTER_H__

#include <routing.h>

namespace cb
{

class Center: public CRouting
{
public:
   Center();
   virtual ~Center();

public:
   virtual int start(const char* ip, const int& port = 0);
   virtual int join(const char* ip, const char* peer_ip, const int& port = 0, const int& peer_port = 0);

public:
   virtual int lookup(const unsigned int& key, Node* n);
   virtual bool has(const unsigned int& id);

private:
   Node m_Center;

private:
   static void* process(void* r);
};

}; // namespace

#endif
