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
   Yunhong Gu [gu@lac.uic.edu], last updated 11/03/2008
*****************************************************************************/


#ifndef __SECTOR_CLIENT_H__
#define __SECTOR_CLIENT_H__

#include <gmp.h>
#include <index.h>
#include <sysstat.h>
#include <pthread.h>

class Client
{
public:
   Client();
   virtual ~Client();

public:
   static int init(const string& server, const int& port);
   static int login(const string& username, const string& password);
   static void logout();
   static int close();

   static int list(const string& path, vector<SNode>& attr);
   static int stat(const string& path, SNode& attr);
   static int mkdir(const string& path);
   static int move(const string& oldpath, const string& newpath);
   static int remove(const string& path);
   static int sysinfo(SysStat& sys);
   static int dataInfo(const vector<string>& files, vector<string>& info);

protected:
   static string revisePath(const string& path);

protected:
   static string m_strServerHost;
   static string m_strServerIP;
   static int m_iServerPort;
   static CGMP m_GMP;
   static int32_t m_iKey;

private:
   static int m_iCount;

protected:
   static int m_iReusePort;
};

typedef Client Sector;

#endif
