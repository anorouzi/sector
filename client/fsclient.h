/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
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
#include <client.h>
#include <udt.h>

namespace cb
{

class CCBFile;

class CFSClient: public Client
{
friend class CCBFile;

public:
   CCBFile* createFileHandle();
   void releaseFileHandle(CCBFile* f);

   int stat(const string& filename, CFileAttr& attr);
};

class CCBFile
{
friend class CFSClient;

private:
   CCBFile();
   virtual ~CCBFile();

public:
   int open(const string& filename, const int& mode = 1, char* cert = NULL);
   int read(char* buf, const int64_t& offset, const int64_t& size);
   int write(const char* buf, const int64_t& offset, const int64_t& size);
   int download(const char* localpath, const bool& cont = false);
   int upload(const char* localpath, const bool& cont = false);
   int close();

private:
   CFSClient* m_pFSClient;

   string m_strServerIP;
   int m_iServerPort;

   CGMP m_GMP;

   string m_strFileName;

   UDTSOCKET m_uSock;
};

}; // namespace

#endif
