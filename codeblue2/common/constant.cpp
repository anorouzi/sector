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

#include "constant.h"

using namespace std;


map<int, string> SectorError::s_mErrorMsg;

SectorError::SectorError()
{
   s_mErrorMsg.clear();
   s_mErrorMsg[-1] = "unknown error.";
   s_mErrorMsg[-1001] = "permission is not allowed for the operation on the specified file/dir.";
   s_mErrorMsg[-1002] = "file/dir alreadt exists.";
   s_mErrorMsg[-1003] = "file/dir does not exist.";
   s_mErrorMsg[-1004] = "file/dir is busy.";
   s_mErrorMsg[-1005] = "a failure happens on the local file system.";
   s_mErrorMsg[-2000] = "security check (certificate/account/password/acl) failed.";
   s_mErrorMsg[-2001] = "no certificate found or wrong certificate.";
   s_mErrorMsg[-2002] = "the account does not exist.";
   s_mErrorMsg[-2003] = "the password is incorrect.";
   s_mErrorMsg[-2004] = "the request is from an illegal IP address.";
   s_mErrorMsg[-2005] = "failed to initialize SSL CTX.";
   s_mErrorMsg[-3000] = "connection fails.";
   s_mErrorMsg[-4000] = "no enough resource (memory/disk) is available.";
   s_mErrorMsg[-5000] = "a timeout event happened.";
   s_mErrorMsg[-6000] = "at least one parameter is invalid.";
   s_mErrorMsg[-6001] = "the operation is not supported.";
}

string SectorError::getErrorMsg(int ecode)
{
   map<int, string>::const_iterator i = s_mErrorMsg.find(ecode);
   if (i == s_mErrorMsg.end())
      return "unknown error.";

   return i->second;      
}
