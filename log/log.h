#ifndef __LOG_H__
#define __LOG_H__

#include <pthread.h>
#include <fstream>

using namespace std;

class CLog
{
public:
   virtual ~CLog() {}
};

class CAccessLog: public CLog
{
public:
   CAccessLog();
   CAccessLog(const char* path);
   virtual ~CAccessLog();

   void insert(const char* ip, const int& port, const char* filename);

private:
   static fstream m_LogFile;
   static pthread_mutex_t m_FAccessLock;
   static int m_iAccessCount;
   static pthread_mutex_t m_FIOLock;
};

class CPerfLog: public CLog
{
public:
   CPerfLog();
   CPerfLog(const char* path);
   virtual ~CPerfLog();

   void insert(const char* ip, const int& port, const char* file, const int& duration, const double& avgRS, const double& avgWS);

private:
   static fstream m_LogFile;
   static pthread_mutex_t m_FAccessLock;
   static int m_iAccessCount;
   static pthread_mutex_t m_FIOLock;
};

class CErrorLog: public CLog
{
public:
   CErrorLog();
   CErrorLog(const char* path);
   virtual ~CErrorLog();

   void insert(const int& module, const int& code, const char* text);

private:
   static fstream m_LogFile;
   static pthread_mutex_t m_FAccessLock;
   static int m_iAccessCount;
   static pthread_mutex_t m_FIOLock;
};

#endif
