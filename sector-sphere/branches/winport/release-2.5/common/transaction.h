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


#ifndef __SECTOR_TRANS_H__
#define __SECTOR_TRANS_H__

#include <set>
#include <vector>
#include <map>
#include <string>
#include "common.h"
#ifndef WIN32
    #include <pthread.h>
#endif

#ifdef WIN32
    #ifdef COMMON_EXPORTS
        #define COMMON_API __declspec(dllexport)
    #else
        #define COMMON_API __declspec(dllimport)
    #endif
#else
    #define COMMON_API
#endif


struct COMMON_API TransType
{
   static const int FILE = 1;
   static const int SPHERE = 2;
   static const int DB = 3;
   static const int REPLICA = 4;
};

struct COMMON_API FileChangeType
{
   static const int32_t FILE_UPDATE_NO = 0;
   static const int32_t FILE_UPDATE_NEW = 1;
   static const int32_t FILE_UPDATE_WRITE = 2;
   static const int32_t FILE_UPDATE_REPLICA = 3;
};

struct COMMON_API Transaction
{
   int m_iTransID;		// unique id
   int m_iType;			// TransType
   int64_t m_llStartTime;	// start time
   std::string m_strFile;	// if type = FILE, this is the file being accessed
   int m_iMode;			// if type = FILE, this is the file access mode
   std::set<int> m_siSlaveID;	// set of slave id involved in this transaction
   int m_iUserKey;		// user key
   int m_iCommand;		// user's command, 110, 201, etc.

   std::map<int, std::string> m_mResults;	// results for write operation
};

class COMMON_API TransManager
{
public:
   TransManager();
   ~TransManager();

public:
   int create(const int type, const int key, const int cmd, const std::string& file, const int mode);
   int addSlave(int transid, int slaveid);
   int retrieve(int transid, Transaction& trans);
   int retrieve(int slaveid, std::vector<int>& trans);
   int updateSlave(int transid, int slaveid);
   int getUserTrans(int key, std::vector<int>& trans);
   int addWriteResult(int transid, int slaveid, const std::string& result);

public:
   unsigned int getTotalTrans();

public:
   std::map<int, Transaction> m_mTransList;	// list of active transactions
   int m_iTransID;				// seed of transaction id

   CMutex m_TLLock;
};

#endif
