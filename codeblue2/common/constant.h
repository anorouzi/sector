/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 01/24/2009
*****************************************************************************/

#ifndef __SECTOR_CONSTANT_H__
#define __SECTOR_CONSTANT_H__

#include <string>
#include <map>

// ERROR codes
class SectorError
{
public:
   static const int E_UNKNOWN = -1;		// unknown error
   static const int E_PERMISSION = -1001;	// no permission for IO
   static const int E_EXIST = -1002;		// file/dir already exist
   static const int E_NOEXIST = -1003;		// file/dir not found
   static const int E_BUSY = -1004;		// file busy
   static const int E_LOCALFILE = -1005;	// local file failure
   static const int E_SECURITY = -2000;		// security check failed
   static const int E_NOCERT = -2001;		// no certificate found
   static const int E_ACCOUNT = -2002;		// account does not exist
   static const int E_PASSWORD = -2003;		// incorrect password
   static const int E_ACL = -2004;		// visit from unallowd IP address
   static const int E_INITCTX = -2005;		// failed to initialize CTX
   static const int E_NOSECSERV = -2006;	// security server is down or cannot connect to it
   static const int E_CONNECTION = - 3000;	// cannot connect to master
   static const int E_RESOURCE = -4000;		// no available resources
   static const int E_TIMEDOUT = -5000;		// timeout
   static const int E_INVALID = -6000;		// invalid parameter
   static const int E_SUPPORT = -6001;		// operation not supported

public:
   static int init();
   static std::string getErrorMsg(int ecode);

private:
   static std::map<int, std::string> s_mErrorMsg;
};

// file open mode
struct SF_MODE
{
   static const int READ = 1;			// read only
   static const int WRITE = 2;			// write only
   static const int RW = 3;			// read and write
   static const int TRUNC = 4;			// trunc the file upon opening
   static const int APPEND = 8;			// move the write offset to the end of the file upon opening
   static const int SECURE = 16;		// encrypted file transfer
   static const int HiRELIABLE = 32;		// replicate data writting at real time (otherwise periodically)
};

//file IO position base
struct SF_POS
{
   static const int BEG = 1;
   static const int CUR = 2;
   static const int END = 3;
};

#endif
