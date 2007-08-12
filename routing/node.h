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


#ifndef __NODE_H__
#define __NODE_H__

namespace cb
{

struct Node
{
   uint32_t m_uiID;
   char m_pcIP[64];
   int32_t m_iPort;
   int32_t m_iAppPort;
};

struct NodeComp
{
   bool operator()(const Node& n1, const Node& n2) const
   {
      int nc = strcmp(n1.m_pcIP, n2.m_pcIP);
      if (nc != 0)
         return (nc > 0);

      return (n1.m_iAppPort > n2.m_iAppPort);
   }
};

struct NodeInfo
{
   int32_t m_iStatus;		// good, bad
   int32_t m_iAvailDisk;	// MB
   int32_t m_iSPEMem;		// MB
   int32_t m_iJobs;		// number of clients currently serves
   int32_t m_iRTT;		// RTT
};

}; // namespace

#endif
