/*****************************************************************************
Copyright (c) 2001 - 2009, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu, last updated 08/01/2009
*****************************************************************************/

#ifndef __UDT_COMMON_H__
#define __UDT_COMMON_H__


#ifndef WIN32
   #include <sys/time.h>
   #include <sys/uio.h>
   #include <pthread.h>
#else
   #include <windows.h>
#endif
#include <cstdlib>
#include "udt.h"


#ifndef WIN32
    #define udt_inet_ntop inet_ntop
    #define udt_inet_pton inet_pton
#else
   // Windows compability
    #include <sys/types.h>
    #include <sys/stat.h>

    #include <sys/timeb.h>
    inline int gettimeofday(struct timeval *tp, void *tzp)
    {
        struct _timeb timebuffer;
    
        errno_t err = _ftime_s (&timebuffer);
        tp->tv_sec = (long)timebuffer.time;
        tp->tv_usec = timebuffer.millitm * 1000;
    
        return err;
    }    

    typedef HANDLE pthread_t;
    typedef HANDLE pthread_mutex_t;
    typedef HANDLE pthread_cond_t;
    typedef DWORD pthread_key_t;

    extern UDT_API void usleep (long usec);

    #define unlink _unlink
    #define snprintf _snprintf_s
    #define atoll _atoi64
    #define stat64 _stat64

    #define     S_ISDIR(m)  (((m)&_S_IFMT) == _S_IFDIR)
    #define     S_ISTYPE(mode, mask)  (((mode) & _S_IFMT) == (mask))
    #define     S_ISREG(mode)    S_ISTYPE((mode), _S_IFREG)

    inline int rand_r (unsigned int * seed)
    {
      long new_seed = (long) *seed;
      if (new_seed == 0)
        new_seed = 0x12345987;
      long temp = new_seed / 127773;
      new_seed = 16807 * (new_seed - temp * 127773) - 2836 * temp;
      if (new_seed < 0)
        new_seed += 2147483647;
     *seed = (unsigned int)new_seed;
      return (int) (new_seed & RAND_MAX);
    }

    inline int log2(float x) {
        int lg;
        if (x <= 0.0f) 
            return 0;
        for(lg = -1; x != 0.0f; x /= 2.0f, lg++);    
        return lg;
    }

    extern "C" UDT_API const char* udt_inet_ntop(int af, const void* s, char* d, int len);
    extern "C" UDT_API int udt_inet_pton(int af, const char* s, void* d);
#endif


////////////////////////////////////////////////////////////////////////////////

//<slr>

class UDT_API CMutex
{
public:
    CMutex()
    {
#ifndef WIN32   // TODO: add error checking
       pthread_mutex_init(&m_Mutex, NULL);
#else
       ::InitializeCriticalSection (&m_Mutex);
#endif
    }

    ~CMutex()
    {
#ifndef WIN32
        pthread_mutex_destroy(&m_Mutex);
#else
        ::DeleteCriticalSection (&m_Mutex);
#endif
    }

    inline bool acquire() {
        bool locked = true;
#ifndef WIN32
        int iret = pthread_mutex_lock(&m_Mutex);
        locked = (iret == 0);
#else
        ::EnterCriticalSection(&m_Mutex);
#endif
        return locked;
    }

    inline bool release() {
        bool unlocked = true;
#ifndef WIN32
        int iret = pthread_mutex_unlock(&m_Mutex);
        unlocked = (iret == 0);
#else
        ::LeaveCriticalSection(&m_Mutex);
#endif
        return unlocked;
    }

    inline bool trylock() {
        bool locked = false;
#ifndef WIN32
        int iret = pthread_mutex_trylock(&m_Mutex);
        locked = (iret == 0);
#else
        BOOL result = ::TryEnterCriticalSection (&m_Mutex);
        locked = (result == TRUE);
#endif
        return locked;
    }


#ifdef WIN32
private:
   CRITICAL_SECTION m_Mutex;           // mutex to be protected

#else
   pthread_mutex_t m_Mutex;            // allow public access for now, to use with pthread_cond_t 
private:
#endif

   CMutex& operator=(const CMutex&);
};

class CMutexGuard
{
public:
    // Automatically lock in constructor
    CMutexGuard(CMutex& mutex)
    : m_Mutex(mutex) {
        m_Locked = m_Mutex.acquire();
    }

    // Automatically unlock in destructor
    ~CMutexGuard()    {
        if (m_Locked) {
            m_Mutex.release();
            m_Locked = false;
        }
    }

private:
   CMutex& m_Mutex;            // Alias name of the mutex to be protected
   bool m_Locked;              // Locking status

   CMutexGuard& operator=(const CMutexGuard&);
};

/*
class CSignal
{
public:
   CSignal();
   ~CSignal();

   bool raise();
   bool wait (unsigned secs, unsigned long msecs);

private:
   pthread_cond_t m_Signal;      // condition to be protected

   CSignal& operator=(const CSignal&);
};
*/

// <slr>


////////////////////////////////////////////////////////////////////////////////

class UDT_API CTimer
{
public:
   CTimer();
   ~CTimer();

public:

      // Functionality:
      //    Sleep for "interval" CCs.
      // Parameters:
      //    0) [in] interval: CCs to sleep.
      // Returned value:
      //    None.

   void sleep(const uint64_t& interval);

      // Functionality:
      //    Seelp until CC "nexttime".
      // Parameters:
      //    0) [in] nexttime: next time the caller is waken up.
      // Returned value:
      //    None.

   void sleepto(const uint64_t& nexttime);

      // Functionality:
      //    Stop the sleep() or sleepto() methods.
      // Parameters:
      //    None.
      // Returned value:
      //    None.

   void interrupt();

      // Functionality:
      //    trigger the clock for a tick, for better granuality in no_busy_waiting timer.
      // Parameters:
      //    None.
      // Returned value:
      //    None.

   void tick();

public:

      // Functionality:
      //    Read the CPU clock cycle into x.
      // Parameters:
      //    0) [out] x: to record cpu clock cycles.
      // Returned value:
      //    None.

   static void rdtsc(uint64_t &x);

      // Functionality:
      //    return the CPU frequency.
      // Parameters:
      //    None.
      // Returned value:
      //    CPU frequency.

   static uint64_t getCPUFrequency();

      // Functionality:
      //    check the current time, 64bit, in microseconds.
      // Parameters:
      //    None.
      // Returned value:
      //    current time in microseconds.

   static uint64_t getTime();

      // Functionality:
      //    trigger an event such as new connection, close, new data, etc. for "select" call.
      // Parameters:
      //    None.
      // Returned value:
      //    None.

   static void triggerEvent();

      // Functionality:
      //    wait for an event to br triggered by "triggerEvent".
      // Parameters:
      //    None.
      // Returned value:
      //    None.

   static void waitForEvent();

private:
   uint64_t m_ullSchedTime;             // next schedulled time

   pthread_cond_t m_TickCond;
   CMutex m_TickLock;

   static pthread_cond_t m_EventCond;
#ifndef WIN32
   static pthread_mutex_t m_EventLock;
#endif

private:
   static uint64_t s_ullCPUFrequency;	// CPU frequency : clock cycles per microsecond
   static uint64_t readCPUFrequency();
};


class UDT_API CGuard
{
public:
   CGuard(pthread_mutex_t& lock);
   ~CGuard();

   static void enterCS(pthread_mutex_t& lock);
   static void leaveCS(pthread_mutex_t& lock);

private:
   pthread_mutex_t& m_Mutex;            // Alias name of the mutex to be protected
   int m_iLocked;                       // Locking status

   CGuard& operator=(const CGuard&);
};



////////////////////////////////////////////////////////////////////////////////

// UDT Sequence Number 0 - (2^31 - 1)

// seqcmp: compare two seq#, considering the wraping
// seqlen: length from the 1st to the 2nd seq#, including both
// seqoff: offset from the 2nd to the 1st seq#
// incseq: increase the seq# by 1
// decseq: decrease the seq# by 1
// incseq: increase the seq# by a given offset

class CSeqNo
{
public:
   inline static const int seqcmp(const int32_t& seq1, const int32_t& seq2)
   {return (abs(seq1 - seq2) < m_iSeqNoTH) ? (seq1 - seq2) : (seq2 - seq1);}

   inline static const int seqlen(const int32_t& seq1, const int32_t& seq2)
   {return (seq1 <= seq2) ? (seq2 - seq1 + 1) : (seq2 - seq1 + m_iMaxSeqNo + 2);}

   inline static const int seqoff(const int32_t& seq1, const int32_t& seq2)
   {
      if (abs(seq1 - seq2) < m_iSeqNoTH)
         return seq2 - seq1;

      if (seq1 < seq2)
         return seq2 - seq1 - m_iMaxSeqNo - 1;

      return seq2 - seq1 + m_iMaxSeqNo + 1;
   }

   inline static const int32_t incseq(const int32_t seq)
   {return (seq == m_iMaxSeqNo) ? 0 : seq + 1;}

   inline static const int32_t decseq(const int32_t& seq)
   {return (seq == 0) ? m_iMaxSeqNo : seq - 1;}

   inline static const int32_t incseq(const int32_t& seq, const int32_t& inc)
   {return (m_iMaxSeqNo - seq >= inc) ? seq + inc : seq - m_iMaxSeqNo + inc - 1;}

public:
   static const int32_t m_iSeqNoTH;             // threshold for comparing seq. no.
   static const int32_t m_iMaxSeqNo;            // maximum sequence number used in UDT
};

////////////////////////////////////////////////////////////////////////////////

// UDT ACK Sub-sequence Number: 0 - (2^31 - 1)

class CAckNo
{
public:
   inline static const int32_t incack(const int32_t& ackno)
   {return (ackno == m_iMaxAckSeqNo) ? 0 : ackno + 1;}

public:
   static const int32_t m_iMaxAckSeqNo;         // maximum ACK sub-sequence number used in UDT
};

////////////////////////////////////////////////////////////////////////////////

// UDT Message Number: 0 - (2^29 - 1)

class CMsgNo
{
public:
   inline static const int msgcmp(const int32_t& msgno1, const int32_t& msgno2)
   {return (abs(msgno1 - msgno2) < m_iMsgNoTH) ? (msgno1 - msgno2) : (msgno2 - msgno1);}

   inline static const int msglen(const int32_t& msgno1, const int32_t& msgno2)
   {return (msgno1 <= msgno2) ? (msgno2 - msgno1 + 1) : (msgno2 - msgno1 + m_iMaxMsgNo + 2);}

   inline static const int msgoff(const int32_t& msgno1, const int32_t& msgno2)
   {
      if (abs(msgno1 - msgno2) < m_iMsgNoTH)
         return msgno2 - msgno1;

      if (msgno1 < msgno2)
         return msgno2 - msgno1 - m_iMaxMsgNo - 1;

      return msgno2 - msgno1 + m_iMaxMsgNo + 1;
   }

   inline static const int32_t incmsg(const int32_t& msgno)
   {return (msgno == m_iMaxMsgNo) ? 0 : msgno + 1;}

public:
   static const int32_t m_iMsgNoTH;             // threshold for comparing msg. no.
   static const int32_t m_iMaxMsgNo;            // maximum message number used in UDT
};

////////////////////////////////////////////////////////////////////////////////

struct CIPAddress
{
   static bool ipcmp(const sockaddr* addr1, const sockaddr* addr2, const int& ver = AF_INET);
   static void ntop(const sockaddr* addr, uint32_t ip[4], const int& ver = AF_INET);
   static void pton(sockaddr* addr, const uint32_t ip[4], const int& ver = AF_INET);
};

////////////////////////////////////////////////////////////////////////////////

struct CMD5
{
   static void compute(const char* input, unsigned char result[16]);
};


#endif
