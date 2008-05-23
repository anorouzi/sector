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
   Yunhong Gu [gu@lac.uic.edu], last updated 04/23/2007
*****************************************************************************/


#ifndef __CONF_H__
#define __CONF_H__

#include <string>
#include <fstream>
#include <vector>
#include <sys/types.h>
#include <stdint.h>

using namespace std;

struct Param
{
   string m_strName;
   vector<string> m_vstrValue;
};

class ConfParser
{
public:
   int init(string path);
   void close();
   int getNextParam(Param& param);

private:
   char* getToken(char* str, string& token);

private:
   ifstream m_ConfFile;
};

class MasterConf
{
public:
   int init(const string& path);

public:
   int m_iServerPort;		// server port
   string m_strSecServIP;	// security server IP
   int m_iSecServPort;		// security server port
   int m_iMaxActiveUser;	// maximum active user
   string m_strHomeDir;		// data directory
   int m_iReplicaNum;		// number of replicas of each file
};

class SlaveConf
{
public:
   int init(const string& path);

public:
   string m_strMasterHost;
   int m_iMasterPort;
   string m_strHomeDir;
   int64_t m_llMaxDataSize;
   int m_iMaxServiceNum;
   string m_strLocalIP;
   string m_strPublicIP;
   int m_iClusterID;
   string m_strExecDir;
};

#endif
