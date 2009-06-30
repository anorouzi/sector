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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/05/2009
*****************************************************************************/


#ifndef __SECTOR_USER_H__
#define __SECTOR_USER_H__

#include <string>
#include <vector>

class ActiveUser
{
public:
   int deserialize(std::vector<std::string>& dirs, const std::string& buf);
   bool match(const std::string& path, int rwx);

public:
   int serialize(char*& buf, int& size);
   int deserialize(const char* buf, const int& size);

public:
   std::string m_strName;			// user name

   std::string m_strIP;				// client IP address
   int m_iPort;					// client port (GMP)
   int m_iDataPort;				// data channel port

   int32_t m_iKey;				// client key

   unsigned char m_pcKey[16];			// client crypto key
   unsigned char m_pcIV[8];			// client crypto iv

   int64_t m_llLastRefreshTime;			// timestamp of last activity
   std::vector<std::string> m_vstrReadList;	// readable directories
   std::vector<std::string> m_vstrWriteList;	// writable directories
   bool m_bExec;				// permission to run Sphere application
};

#endif
