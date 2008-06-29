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


#ifndef __SECTOR_LOG_H__
#define __SECTOR_LOG_H__

#include <pthread.h>
#include <fstream>

class SectorLog
{
public:
   SectorLog();
   ~SectorLog();

public:
   int init(const char* path);
   void close();

   void insert(const char* text);

private:
   std::ofstream m_LogFile;
   pthread_mutex_t m_LogLock;
};

#endif
