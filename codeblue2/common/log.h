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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


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
