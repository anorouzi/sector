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


#ifndef __CBFS_CLIENT_H__
#define __CBFS_CLIENT_H__

#include <gmp.h>
#include <index.h>
#include <file.h>
#include <node.h>
#include <transport.h>
#include <client.h>

namespace cb 
{

class File
{
friend class Client;

private:
   File();
   virtual ~File();

public:
   int open(const string& filename, const int& mode = 1, char* cert = NULL, char* nl = NULL, int nlsize = 0);
   int read(char* buf, const int64_t& offset, const int64_t& size);
   int readridx(char* index, const int64_t& offset, const int64_t& rows);
   int write(const char* buf, const int64_t& offset, const int64_t& size);
   int download(const char* localpath, const bool& cont = false);
   int upload(const char* localpath, const bool& cont = false);
   int close();

private:
   string m_strServerIP;
   int m_iServerPort;

   CGMP m_GMP;
   Transport m_DataChn;

   string m_strFileName;
};

}; // namespace

#endif
