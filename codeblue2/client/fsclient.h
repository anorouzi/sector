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
   Yunhong Gu [gu@lac.uic.edu], last updated 04/29/2008
*****************************************************************************/

#ifndef __SECTOR_FS_CLIENT_H__
#define __SECTOR_FS_CLIENT_H__

#include <gmp.h>
#include <index.h>
#include <transport.h>
#include <client.h>

class SectorFile: public Client
{
public:
   SectorFile() {}
   virtual ~SectorFile() {}

public:
   int open(const string& filename, const int& mode = 1);
   int read(char* buf, const int64_t& offset, const int64_t& size);
   int readridx(char* index, const int64_t& offset, const int64_t& rows);
   int write(const char* buf, const int64_t& offset, const int64_t& size);
   int download(const char* localpath, const bool& cont = false);
   int upload(const char* localpath, const bool& cont = false);
   int close();

private:
   Transport m_DataChn;

   string m_strFileName;
};

#endif
