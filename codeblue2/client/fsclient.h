/*****************************************************************************
Copyright © 2006- 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 12/28/2008
*****************************************************************************/

#ifndef __SECTOR_FS_CLIENT_H__
#define __SECTOR_FS_CLIENT_H__

#include <gmp.h>
#include <index.h>
#include <transport.h>
#include <client.h>

enum SF_MODE{READ = 1, WRITE = 2, RW = 3, TRUNC = 4, APPEND = 8};
enum SF_POS{BEG, CUR, END};

class SectorFile: public Client
{
public:
   SectorFile() {}
   virtual ~SectorFile() {}

public:
   int open(const string& filename, SF_MODE = READ);
   int64_t read(char* buf, const int64_t& size);
   int64_t write(const char* buf, const int64_t& size);
   int download(const char* localpath, const bool& cont = false);
   int upload(const char* localpath, const bool& cont = false);
   int close();

   int seekp(int64_t off, SF_POS pos);
   int seekg(int64_t off, SF_POS pos);
   int64_t tellp();
   int64_t tellg();
   bool eof();

private:
   Transport m_DataChn;

   string m_strFileName;

   int64_t m_llSize;
   int64_t m_llCurReadPos;
   int64_t m_llCurWritePos;
};

#endif
