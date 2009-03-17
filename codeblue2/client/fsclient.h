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
   Yunhong Gu [gu@lac.uic.edu], last updated 12/28/2008
*****************************************************************************/

#ifndef __SECTOR_FS_CLIENT_H__
#define __SECTOR_FS_CLIENT_H__

#include <gmp.h>
#include <index.h>
#include <constant.h>
#include <client.h>

class SectorFile: public Client
{
public:
   SectorFile() {}
   virtual ~SectorFile() {}

public:
   int open(const string& filename, int mode = SF_MODE::READ);
   int64_t read(char* buf, const int64_t& size);
   int64_t write(const char* buf, const int64_t& size);
   int download(const char* localpath, const bool& cont = false);
   int upload(const char* localpath, const bool& cont = false);
   int close();

   int seekp(int64_t off, int pos = SF_POS::BEG);
   int seekg(int64_t off, int pos = SF_POS::BEG);
   int64_t tellp();
   int64_t tellg();
   bool eof();

private:
   int32_t m_iSession;
   std::string m_strSlaveIP;
   int32_t m_iSlaveDataPort;

   unsigned char m_pcKey[16];
   unsigned char m_pcIV[8];

   string m_strFileName;

   int64_t m_llSize;
   int64_t m_llCurReadPos;
   int64_t m_llCurWritePos;

   bool m_bRead;
   bool m_bWrite;
   bool m_bSecure;
};

#endif
