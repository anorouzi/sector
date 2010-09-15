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
   Yunhong Gu, last updated 09/14/2010
*****************************************************************************/

#include <conf.h>
#include <utility.h>
#include <iostream>

using namespace std;

void Utility::print_error(int code)
{
   cerr << "ERROR: " << code << " " << SectorError::getErrorMsg(code) << endl;
}

int Utility::login(Sector& client)
{
   Session s;
   s.loadInfo("../conf/client.conf");

   int result = 0;

   if ((result = client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort)) < 0)
   {
      print_error(result);
      return -1;
   }
   if ((result = client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str())) < 0)
   {
      print_error(result);
      return -1;
   }

   return 0;
}

int Utility::logout(Sector& client)
{
   client.logout();
   client.close();
   return 0;
}
