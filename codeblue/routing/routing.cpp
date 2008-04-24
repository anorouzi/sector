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


#include <routing.h>
#include <dhash.h>

using namespace cb;

CRouting::CRouting():
m_iAppPort(0),
m_iKeySpace(32),
m_pGMP(NULL)
{
}

CRouting::~CRouting()
{
}

void CRouting::setAppPort(const int& port)
{
   m_iAppPort = port;
}

uint32_t CRouting::hash(const char* ip, const int& port)
{
   char str[64];
   sprintf(str, "%s:%d", ip, port);
   return DHash::hash(str, m_iKeySpace);
}
