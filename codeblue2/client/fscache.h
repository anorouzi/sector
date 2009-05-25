/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/


This file is part of Sector Client.

The Sector Client is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

The Sector Client is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 05/08/2009
*****************************************************************************/

#ifndef __SECTOR_FS_CACHE_H__
#define __SECTOR_FS_CACHE_H__

#include <index.h>
#include <string>
#include <map>

struct StatRec
{
   int m_iCount;
   int m_bChange;
   int64_t m_llTimeStamp;
   int64_t m_llSize;
};

class StatCache
{
public:
   StatCache() {}
   virtual ~StatCache() {}

public:
   void insert(const std::string& path);
   void update(const std::string& path, const int64_t& ts, const int64_t& size);
   int stat(const std::string& path, SNode& attr);
   void remove(const std::string& path);

private:
   std::map<std::string, StatRec> m_mOpenedFiles;
};

#endif
