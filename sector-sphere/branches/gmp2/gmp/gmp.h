/*****************************************************************************
Copyright (c) 2005 - 2010, The Board of Trustees of the University of Illinois.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

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

/*****************************************************************************
written by
   Yunhong Gu, last updated 12/31/2010
*****************************************************************************/


#ifndef __GMP_H__
#define __GMP_H__

#ifndef WIN32
   #include <arpa/inet.h>
   #include <netinet/in.h>
   #include <pthread.h>
   #include <sys/socket.h>
   #include <sys/stat.h>
   #include <sys/time.h>
   #include <sys/types.h>

   #define GMP_API
#else
   #include <windows.h>

   #ifdef GMP_EXPORTS
      #define GMP_API __declspec(dllexport)
   #else
      #define GMP_API __declspec(dllimport)
   #endif
   #pragma warning( disable: 4251 )
#endif

#include <cstring>
#include <list>
#include <map>
#include <queue>
#include <string>
#include <vector>

#include "message.h"
#include "prec.h"
//TODO: remove dependency on sector.h
#include "sector.h"


class CGMP;

class CGMPMessage
{
friend class CGMP;

public:
   CGMPMessage();
   CGMPMessage(const CGMPMessage& msg);
   ~CGMPMessage();

   int32_t m_piHeader[6];
   int32_t& m_iType;            // 0 Data; 1 ACK; 2 RTT; 3 UDT Rendezvous connection
   int32_t& m_iSession;         // Used to differentiate GMP instances on the same address
   int32_t& m_iSrcChn;          // source channel, multiple channels for the same GMP instance
   int32_t& m_iDstChn;          // destination channel
   int32_t& m_iID;              // message ID
   int32_t& m_iInfo;            // additional information, depending on type

   char* m_pcData;
   int m_iLength;

public:
   void pack(const char* data, const int& len, const int32_t& info = 0,
             const int& src_chn = 0, const int& dst_chn = 0);
   void pack(const int32_t& type, const int32_t& info = 0,
             const int& src_chn = 0, const int& dst_chn = 0);

public:
   static int32_t g_iSession;

private:
   static int32_t initSession();

   static int32_t g_iID;
   static pthread_mutex_t g_IDLock;
   static const int32_t g_iMaxID = 0xFFFFFFF;
   static const int g_iHdrField;
   static const int g_iHdrSize;
};

struct CMsgRecord
{
   std::string m_strIP;
   int m_iPort;
   CGMPMessage* m_pMsg;
   int64_t m_llTimeStamp;
};

struct CFMsgRec
{
   bool operator()(const CMsgRecord* m1, const CMsgRecord* m2) const
   {
      int res = strcmp(m1->m_strIP.c_str(), m2->m_strIP.c_str());
      if (0 == res)
      {
         if (m1->m_iPort == m2->m_iPort)
            return m1->m_pMsg->m_iID > m2->m_pMsg->m_iID;
         return (m1->m_iPort > m2->m_iPort);
      }
      return (res > 0);
   }
};

struct CChannelRec
{
   CChannelRec();
   ~CChannelRec();

   int m_iID;
   std::queue<CMsgRecord*> m_qRcvQueue;
   std::map<int32_t, CMsgRecord*> m_mResQueue;

   pthread_mutex_t m_RcvQueueLock;
   pthread_cond_t m_RcvQueueCond;
   pthread_mutex_t m_ResQueueLock;
   pthread_cond_t m_ResQueueCond;

   int m_iRefCount;
};

class GMP_API CGMP
{
public:
   CGMP();
   ~CGMP();

public:
   int init(const int& port = 0);
   int close();

   // When GMP is initialized with port 0, it will pick up any random available port.
   // This function can retrive the port.
   int getPort();

   // Return the next available channel ID. Each GMP can support communications on
   // multiple independent channels. The default channel is 0 and it is always available.
   // The channel cannot be used unless this API is called.
   // Return channel ID to system when it is not used anymore. 
   int createChn();
   int releaseChn(int chn);

public:
   int sendto(const std::string& ip, const int& port, int32_t& id, const CUserMessage* msg,
              const int& src_chn = 0, const int& dst_chn = 0);
   int recvfrom(std::string& ip, int& port, int32_t& id, CUserMessage* msg, const bool& block = true,
                int* src_chn = NULL, const int& dst_chn = 0);
   int recv(const int32_t& id, CUserMessage* msg, int* src_chn = NULL, const int& dst_chn = 0);
   int rpc(const std::string& ip, const int& port, CUserMessage* req, CUserMessage* res,
           const int& src_chn = 0, const int& dst_chn = 0);
   int multi_rpc(const std::vector<Address>& dest, CUserMessage* req, std::vector<CUserMessage*>* res = NULL,
                 const int& src_chn = 0, const int& dst_chn = 0);
   int rtt(const std::string& ip, const int& port, const bool& clear = false);

private: // Send data using UDP or UDT.
   int UDPsend(const char* ip, const int& port, int32_t& id, const int& src_chn, const int& dst_chn, 
               const char* data, const int& len, const bool& reliable = true);
   int UDPsend(const char* ip, const int& port, CGMPMessage* msg);
   int UDTsend(const char* ip, const int& port, int32_t& id, const int& src_chn, const int& dst_chn,
               const char* data, const int& len);
   int UDTsend(const char* ip, const int& port, CGMPMessage* msg);

private: // UDT helper functions, see udt_helper.cpp.
   int UDTCreate(UDTSOCKET& usock);
   int UDTConnect(const UDTSOCKET& usock, const char* ip, const int& port);
   int UDTSend(const UDTSOCKET& usock, const char* buf, const int& size);
   int UDTRecv(const UDTSOCKET& usock, char* buf, const int& size);

private: // channel operations
   CChannelRec* getChnHandle(int id);
   int releaseChnHandle(CChannelRec* chn);
   void storeMsg(int info, CChannelRec* chn, CMsgRecord* rec, int& qsize);

private:
   pthread_t m_SndThread;
   pthread_t m_RcvThread;
   pthread_t m_UDTRcvThread;
#ifndef WIN32
   static void* sndHandler(void*);
   static void* rcvHandler(void*);
   static void* udtRcvHandler(void*);
#else
   static DWORD WINAPI sndHandler(LPVOID);
   static DWORD WINAPI rcvHandler(LPVOID);
   static DWORD WINAPI udtRcvHandler(LPVOID);
#endif

   pthread_mutex_t m_SndQueueLock;
   pthread_cond_t m_SndQueueCond;
   pthread_mutex_t m_RTTLock;
   pthread_cond_t m_RTTCond;

private:
   int m_iPort;					// GMP port number
   SYSSOCKET m_UDPSocket;			// UDP socket, for small msg
   UDTSOCKET m_UDTSocket;			// UDT socket, connection cached, for large msg
   int m_iUDTReusePort;				// UDT port number
   int m_iUDTEPollID;				// UDT EPoll ID

   std::list<CMsgRecord*> m_lSndQueue;
   std::map<int, CChannelRec*> m_mCurrentChn;   // Current channel list, including (always) default channel 0.
   pthread_mutex_t m_ChnLock;
   CPeerMgmt m_PeerHistory;

   volatile bool m_bInit;
   volatile bool m_bClosed;

   int m_iChnIDSeed;                            // A seed value to generate next channel ID (local unique).

private:
   static const int m_iMaxUDPMsgSize = 1456;
};

#endif
