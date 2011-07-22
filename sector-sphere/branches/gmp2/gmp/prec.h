/*****************************************************************************
Copyright (c) 2011, VeryCloud LLC. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

* Neither the name of the VeryCloud LLC nor the names of it scontributors
  may be used to endorse or promote products derived from this software
  without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#ifndef __GMP_PREC_H__
#define __GMP_PREC_H__

#include <list>
#include <map>
#include <set>
#include <string>

#include "cache.h"
#include "common.h"
#include "udt.h"

// Message record, to avoid repeated messages.
class CPeerRecord
{
public:
   CPeerRecord();

public:
   std::string m_strIP;
   int m_iPort;
   int m_iSession;
   int32_t m_iID;
   int64_t m_llTimeStamp;
   int32_t m_iFlowWindow;

public:
   CPeerRecord& operator=(CPeerRecord& obj);
   bool operator==(CPeerRecord& obj);
   CPeerRecord* clone();
   int getKey();
   void release() {}
};

// RTT cache.
class CPeerRTT
{
public:
   std::string m_strIP;
   int m_iRTT;
   int64_t m_llTimeStamp;

public:
   CPeerRTT& operator=(CPeerRTT& obj);
   bool operator==(CPeerRTT& obj);
   CPeerRTT* clone();
   int getKey();
   void release() {}
};

// Persistent UDT connections.
class CUDTConns
{
public:
   std::string m_strIP;
   int m_iPort;
   // On the same IP:port address, a GMP may be stoped and started multiple times,
   // we need a 3rd id to differentiate these connections.
   // This usually happens when a client terminate itself and then restart immediately.
   int m_iSession;
   UDTSOCKET m_UDT;
   int64_t m_llTimeStamp;

public:
   CUDTConns& operator=(CUDTConns& obj);
   bool operator==(CUDTConns& obj);
   CUDTConns* clone();
   int getKey();
   void release();
};

class CPeerManagement
{
public:
   CPeerManagement();
   ~CPeerManagement();

public:
   void insert(const std::string& ip, const int& port, const int& session, const int32_t& id = -1,
               const int& rtt = -1, const int& fw = 0);

   bool hit(const std::string& ip, const int& port, const int& session, const int32_t& id);

   int flowControl(const std::string& ip, const int& port, const int& session);

   int getRTT(const std::string& ip);
   void clearRTT(const std::string& ip);

   int setUDTSocket(const std::string& ip, const int& port, const UDTSOCKET& usock);
   int getUDTSocket(const std::string& ip, const int& port, UDTSOCKET& usock);

private:
   int addRecentPR(const CPeerRecord& pr);

private:
   CCache<CPeerRecord> m_RecentRec;
   CCache<CPeerRTT> m_PeerRTT;
   CCache<CUDTConns> m_PersistentUDT;

   pthread_mutex_t m_PeerRecLock;

private:
   static const unsigned int m_uiHashSpace = 20;
   static const unsigned int m_uiRecLimit = 65536;

public:
   static int32_t hash(const std::string& ip, const int& port, const int& session, const int32_t& id);
   static int32_t hash(const std::string& ip, const int& port);
   static int32_t hash(const std::string& ip);
};

#endif
