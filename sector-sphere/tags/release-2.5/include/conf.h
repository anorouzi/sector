/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/


#ifndef __SECTOR_CONF_H__
#define __SECTOR_CONF_H__

#include <map>
#include <string>
#include <fstream>
#include <vector>
#include <sys/types.h>
#include <udt.h>
#ifndef WIN32
   #include <stdint.h>
#endif

#ifndef WIN32
   #define SECTOR_API
#else
   #ifdef SECTOR_EXPORTS
      #define SECTOR_API __declspec(dllexport)
   #else
      #define SECTOR_API __declspec(dllimport)
   #endif
   #pragma warning( disable: 4251 )
#endif

class SECTOR_API ConfLocation
{
public:
   static int locate(std::string& loc);
};

struct SECTOR_API Param
{
   std::string m_strName;
   std::vector<std::string> m_vstrValue;
};

class SECTOR_API ConfParser
{
public:
   int init(const std::string& path);
   void close();
   int getNextParam(Param& param);

private:
   char* getToken(char* str, std::string& token);

private:
   std::ifstream m_ConfFile;
   std::vector<std::string> m_vstrLines;
   std::vector<std::string>::iterator m_ptrLine;
   int m_iLineCount;
};

enum MetaForm {MEMORY = 1, DISK};

class MasterConf
{
public:
   MasterConf();

   int init(const std::string& path);

public:
   int m_iServerPort;			// server port
   std::string m_strSecServIP;		// security server IP
   int m_iSecServPort;			// security server port
   int m_iMaxActiveUser;		// maximum active user
   std::string m_strHomeDir;		// data directory
   int m_iReplicaNum;			// number of replicas of each file
   MetaForm m_MetaType;			// form of metadata
   int m_iSlaveTimeOut;			// slave timeout threshold
   int64_t m_llSlaveMinDiskSpace;	// minimum available disk space allowed on each slave
   int m_iClientTimeOut;		// client timeout threshold
   int m_iLogLevel;			// level of logs, higher = more verbose, 0 = no log
};

class SlaveConf
{
public:
   SlaveConf();

   int init(const std::string& path);

public:
   std::string m_strMasterHost;
   int m_iMasterPort;
   std::string m_strHomeDir;
   int64_t m_llMaxDataSize;
   int m_iMaxServiceNum;
   std::string m_strLocalIP;
   std::string m_strPublicIP;
   int m_iClusterID;
   MetaForm m_MetaType;		// form of metadata
};

class SECTOR_API ClientConf
{
public:
   ClientConf();

   int init(const std::string& path);

public:
   std::string m_strUserName;
   std::string m_strPassword;
   std::string m_strMasterIP;
   int m_iMasterPort;
   std::string m_strCertificate;
   int64_t m_llMaxCacheSize;
   int m_iFuseReadAheadBlock;
   int64_t m_llMaxWriteCacheSize;
};

#endif
