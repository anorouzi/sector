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
   Yunhong Gu [gu@lac.uic.edu], last updated 07/09/2008
*****************************************************************************/

#ifndef __SECTOR_CONSTANT_H__
#define __SECTOR_CONSTANT_H__

enum IOMODE {READ = 1, WRITE = 2, EXEC = 4};

// ERROR codes
struct SectorError
{
   static const int E_UNKNOWN = -1;		// unknown error
   static const int E_PERMISSION = -1001;	// no permission for IO
   static const int E_EXIST = -1002;		// file/dir already exist
   static const int E_NOEXIST = -1003;		// file/dir not found
   static const int E_BUSY = -1004;		// file busy
   static const int E_SECURITY = -2000;		// security check failed
   static const int E_RESOURCE = -3000;		// no available resources
   static const int E_TIMEDOUT = -4000;		// timeout
   static const int E_INVALID = -5000;		// invalid parameter
};

#endif
