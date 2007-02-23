/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#include <log.h>
#include <time.h>
#include <string>
#include <iostream>

using namespace std;


fstream CAccessLog::m_LogFile;
pthread_mutex_t CAccessLog::m_FAccessLock = PTHREAD_MUTEX_INITIALIZER;
int CAccessLog::m_iAccessCount = 0;
pthread_mutex_t CAccessLog::m_FIOLock = PTHREAD_MUTEX_INITIALIZER;

CAccessLog::CAccessLog()
{
   pthread_mutex_lock(&m_FAccessLock);

   if (0 == m_iAccessCount)
      m_LogFile.open("access.log", ios::out | ios::app | ios::ate);

   ++ m_iAccessCount;

   pthread_mutex_unlock(&m_FAccessLock);
}

CAccessLog::CAccessLog(const char* path)
{
   pthread_mutex_lock(&m_FAccessLock);
   m_LogFile.open(path, ios::out | ios::app | ios::ate);
   pthread_mutex_unlock(&m_FAccessLock);
}

CAccessLog::~CAccessLog()
{
   pthread_mutex_lock(&m_FAccessLock);

   -- m_iAccessCount;

   if (0 == m_iAccessCount)
      m_LogFile.close();

   pthread_mutex_unlock(&m_FAccessLock);
}

void CAccessLog::insert(const char* ip, const int& port, const char* filename)
{
   time_t ts = time(NULL);
   char tf[64];
   strcpy(tf, ctime(&ts));
   tf[strlen(tf) - 1] = '\0';

   m_LogFile << tf << "\t" << ip << "\t" << port << "\t" << filename << endl;
   m_LogFile.flush();
}


fstream CPerfLog::m_LogFile;
pthread_mutex_t CPerfLog::m_FAccessLock = PTHREAD_MUTEX_INITIALIZER;
int CPerfLog::m_iAccessCount = 0;
pthread_mutex_t CPerfLog::m_FIOLock = PTHREAD_MUTEX_INITIALIZER;

CPerfLog::CPerfLog()
{
   pthread_mutex_lock(&m_FAccessLock);

   if (0 == m_iAccessCount)
      m_LogFile.open("perf.log", ios::out | ios::app | ios::ate);

   ++ m_iAccessCount;

   pthread_mutex_unlock(&m_FAccessLock);
}

CPerfLog::CPerfLog(const char* path)
{
   pthread_mutex_lock(&m_FAccessLock);
   m_LogFile.open(path, ios::out | ios::app | ios::ate);
   pthread_mutex_unlock(&m_FAccessLock);
}

CPerfLog::~CPerfLog()
{
   pthread_mutex_lock(&m_FAccessLock);

   -- m_iAccessCount;

   if (0 == m_iAccessCount)
      m_LogFile.close();

   pthread_mutex_unlock(&m_FAccessLock);
}

void CPerfLog::insert(const char* ip, const int& port, const char* file, const int& duration, const double& avgRS, const double& avgWS)
{
   time_t ts = time(NULL);
   char tf[64];
   strcpy(tf, ctime(&ts));
   tf[strlen(tf) - 1] = '\0';

   m_LogFile << tf << "\t" << ip << "\t" << port << "\t" << file << "\t" << duration << "\t" << avgRS << "\t" << avgWS << endl;
   m_LogFile.flush();
}


fstream CErrorLog::m_LogFile;
pthread_mutex_t CErrorLog::m_FAccessLock = PTHREAD_MUTEX_INITIALIZER;
int CErrorLog::m_iAccessCount = 0;
pthread_mutex_t CErrorLog::m_FIOLock = PTHREAD_MUTEX_INITIALIZER;

CErrorLog::CErrorLog()
{
   pthread_mutex_lock(&m_FAccessLock);

   if (0 == m_iAccessCount)
      m_LogFile.open("error.log", ios::out | ios::app | ios::ate);

   ++ m_iAccessCount;

   pthread_mutex_unlock(&m_FAccessLock);
}

CErrorLog::CErrorLog(const char* path)
{
   pthread_mutex_lock(&m_FAccessLock);

   if (0 == m_iAccessCount)
      m_LogFile.open(path, ios::out | ios::app | ios::ate);

   ++ m_iAccessCount;

   pthread_mutex_unlock(&m_FAccessLock);
}

CErrorLog::~CErrorLog()
{
   pthread_mutex_lock(&m_FAccessLock);

   -- m_iAccessCount;

   if (0 == m_iAccessCount)
      m_LogFile.close();

   pthread_mutex_unlock(&m_FAccessLock);
}

void CErrorLog::insert(const int& module, const int& code, const char* text)
{
   pthread_mutex_lock(&m_FIOLock);

   time_t ts = time(NULL);
   char tf[64];
   strcpy(tf, ctime(&ts));
   tf[strlen(tf) - 1] = '\0';

   m_LogFile << module << "\t" << code << "\t" << text << endl;

   pthread_mutex_unlock(&m_FIOLock);
}
