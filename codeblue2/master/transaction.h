/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/23/2008
*****************************************************************************/


#ifndef __SECTOR_TRANS_H__
#define __SECTOR_TRANS_H__

#include <map>
#include <string>

struct Transaction
{
   int m_iTransID;
   int64_t m_llStartTime;
   std::string m_strFile;
   int m_iMode;
   int m_iSlaveID;
   std::string m_strUser;
   int m_iCommand;
};

class TransManager
{
public:
   TransManager();
   ~TransManager();

public:
   int insert(const int slave, const std::string& file, const int mode, const std::string& user, const int cmd);
   int retrieve(int transid, Transaction& trans);
   int update(int transid);

public:
   unsigned int getTotalTrans();

private:
   std::map<int, Transaction> m_mTransList;
   int m_iTransID;
};

#endif
