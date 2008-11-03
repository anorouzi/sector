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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/29/2008
*****************************************************************************/


#include "log.h"
#include <time.h>
#include <string>
#include <iostream>

using namespace std;

SectorLog::SectorLog()
{
   pthread_mutex_init(&m_LogLock, NULL);
}

SectorLog::~SectorLog()
{
   pthread_mutex_destroy(&m_LogLock);
}

int SectorLog::init(const char* path)
{
   m_LogFile.open(path, ios::trunc);

   if (m_LogFile.bad() || m_LogFile.fail())
      return -1;

   return 0;
}

void SectorLog::close()
{
   m_LogFile.close();
}

void SectorLog::insert(const char* text)
{
   pthread_mutex_lock(&m_LogLock);

   time_t t = time(NULL);
   char ct[64];
   sprintf(ct, "%s", ctime(&t));
   ct[strlen(ct) - 1] = '\0';
   m_LogFile << ct << "\t" << text << endl;
   m_LogFile.flush();

   pthread_mutex_unlock(&m_LogLock);
}

void SectorLog::logUserActivity(const char* user, const char* ip, const char* cmd, const char* file, const char* res, const char* slave)
{
   char* text = new char[128 + strlen(file)];
   sprintf(text, "USER: %s  IP: %s  CMD: %s  FILE/DIR: %s  RESULT: %s  SLAVE: %s", user, ip, cmd, file, res, slave);
   insert(text);
   delete [] text;
}
