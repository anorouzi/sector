/*****************************************************************************
Copyright 2011 The Sector Alliance

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
   bdl62, last updated 05/21/2011
*****************************************************************************/

#include <assert.h>
#include <cstring>
#include <iostream>

#include "message.h"

using namespace std;
using namespace sector;

int test1()
{
   CliLoginReq req;
   CliLoginReq restored_req;

   req.m_strUser = "11111111";
   req.m_strPasswd = "22222222";
   for (int i = 0; i < 8; ++ i)
      req.m_pcCryptoIV[i] = static_cast<char>(i);

   req.serialize();
   restored_req.setData(0, req.getData(), req.m_iDataLength);
   restored_req.deserialize();

   assert(restored_req.m_strUser == req.m_strUser);
   assert(restored_req.m_strPasswd == req.m_strPasswd);
   for (int i = 0; i < 8; ++ i)
      assert(restored_req.m_pcCryptoIV[i] == req.m_pcCryptoIV[i]);

   return 0;
}

int test2()
{
   CliLoginRes res;
   CliLoginRes restored_res;

   res.m_iCliKey = 1;
   res.m_iCliToken = 2;
   Address m;
   m.m_strIP = "10.0.0.1";
   m.m_iPort = 5000;
   res.m_mMasters[1] = m;

   res.serialize();
   restored_res.setData(0, res.getData(), res.m_iDataLength);
   restored_res.deserialize();

   assert(restored_res.m_iCliKey == res.m_iCliKey);
   assert(restored_res.m_mMasters.size() == 1);
   const Address& restored_m = restored_res.m_mMasters.begin()->second;
   assert(m.m_strIP == restored_m.m_strIP);
   assert(m.m_iPort == restored_m.m_iPort);

   return 0;
}

int main()
{
   test1();
   test2();

   return 0;
}
