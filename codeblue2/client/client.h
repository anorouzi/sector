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
   Yunhong Gu [gu@lac.uic.edu], last updated 01/22/2009
*****************************************************************************/


#ifndef __SECTOR_CLIENT_H__
#define __SECTOR_CLIENT_H__

#include <gmp.h>
#include <index.h>
#include <sysstat.h>
#include <topology.h>
#include <constant.h>
#include <pthread.h>
#include <datachn.h>
#include <routing.h>
#include "fscache.h"

class Client
{
public:
   Client();
   virtual ~Client();

public:
   static int init(const std::string& server, const int& port);
   static int login(const std::string& username, const std::string& password, const char* cert = NULL);
   static int logout();
   static int close();

   static int list(const std::string& path, std::vector<SNode>& attr);
   static int stat(const std::string& path, SNode& attr);
   static int mkdir(const std::string& path);
   static int move(const std::string& oldpath, const std::string& newpath);
   static int remove(const std::string& path);
   static int copy(const std::string& src, const std::string& dst);
   static int utime(const std::string& path, const int64_t& ts);

   static int sysinfo(SysStat& sys);

public:
   static int dataInfo(const std::vector<std::string>& files, std::vector<std::string>& info);

protected:
   static std::string revisePath(const std::string& path);
   static int updateMasters();

protected:
   static std::string g_strServerHost;
   static std::string g_strServerIP;
   static int g_iServerPort;
   static CGMP g_GMP;
   static DataChn g_DataChn;
   static int32_t g_iKey;

   // this is the global key/iv for this client. do not use this for any connection; a new connection should duplicate this
   static unsigned char g_pcCryptoKey[16];
   static unsigned char g_pcCryptoIV[8];

   static Topology g_Topology;

   static SectorError g_ErrorInfo;

   static StatCache g_StatCache;

private:
   static int g_iCount;

protected: // master routing
   static Routing g_Routing;
};

typedef Client Sector;

#endif
