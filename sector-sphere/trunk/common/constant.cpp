/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 08/16/2010
*****************************************************************************/

#include <sector.h>

using namespace std;


map<int, string> SectorError::s_mErrorMsg;

int SectorError::init()
{
   s_mErrorMsg.clear();
   s_mErrorMsg[-1] = "unknown error.";
   s_mErrorMsg[-1001] = "permission is not allowed for the operation on the specified file/dir.";
   s_mErrorMsg[-1002] = "file/dir already exists.";
   s_mErrorMsg[-1003] = "file/dir does not exist.";
   s_mErrorMsg[-1004] = "file/dir is busy.";
   s_mErrorMsg[-1005] = "a failure happens on the local file system.";
   s_mErrorMsg[-1006] = "directory is not empty.";
   s_mErrorMsg[-1007] = "directory does not exist or not a directory.";
   s_mErrorMsg[-1008] = "this file is not openned yet for IO operations.";
   s_mErrorMsg[-1009] = "all replicas have been lost.";
   s_mErrorMsg[-2000] = "security check (certificate/account/password/acl) failed.";
   s_mErrorMsg[-2001] = "no certificate found or wrong certificate.";
   s_mErrorMsg[-2002] = "the account does not exist.";
   s_mErrorMsg[-2003] = "the password is incorrect.";
   s_mErrorMsg[-2004] = "the request is from an illegal IP address.";
   s_mErrorMsg[-2005] = "failed to initialize SSL CTX.";
   s_mErrorMsg[-2006] = "no response from security server.";
   s_mErrorMsg[-2007] = "client timeout and was kicked out by server.";
   s_mErrorMsg[-2008] = "no authority to run the command.";
   s_mErrorMsg[-2009] = "invalid network address.";
   s_mErrorMsg[-2010] = "unable to initailize GMP.";
   s_mErrorMsg[-2011] = "unable to initialize data channel.";
   s_mErrorMsg[-2012] = "unable to retrieve master certificate";
   s_mErrorMsg[-2013] = "all masters have been lost";
   s_mErrorMsg[-3000] = "connection fails.";
   s_mErrorMsg[-3001] = "data connection has been lost";
   s_mErrorMsg[-4000] = "no enough resource (memory/disk) is available.";
   s_mErrorMsg[-4001] = "no enough disk space.";
   s_mErrorMsg[-5000] = "a timeout event happened.";
   s_mErrorMsg[-6000] = "at least one parameter is invalid.";
   s_mErrorMsg[-6001] = "the operation is not supported.";
   s_mErrorMsg[-6002] = "operation was canceled.";
   s_mErrorMsg[-7001] = "at least one bucket process has failed.";
   s_mErrorMsg[-7002] = "no sphere process is running.";
   s_mErrorMsg[-7003] = "at least one input file cannot be located.";
   s_mErrorMsg[-7004] = "missing index files.";
   s_mErrorMsg[-7005] = "all SPE has failed.";
   s_mErrorMsg[-7006] = "cannot locate any bucket.";

   return s_mErrorMsg.size();
}

string SectorError::getErrorMsg(int ecode)
{
   map<int, string>::const_iterator i = s_mErrorMsg.find(ecode);
   if (i == s_mErrorMsg.end())
      return "unknown error.";

   return i->second;      
}
