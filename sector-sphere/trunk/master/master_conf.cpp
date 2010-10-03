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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/

#include <master.h>
#include <iostream>

using namespace std;

MasterConf::MasterConf():
m_iServerPort(0),
m_strSecServIP(),
m_iSecServPort(0),
m_iMaxActiveUser(1024),
m_strHomeDir("./"),
m_iReplicaNum(1),
m_MetaType(MEMORY),
m_iSlaveTimeOut(300),
m_llSlaveMinDiskSpace(10000000000LL),
m_iClientTimeOut(600),
m_iLogLevel(1)
{
}

int MasterConf::init(const string& path)
{
   ConfParser parser;
   Param param;

   if (0 != parser.init(path))
      return -1;

   while (parser.getNextParam(param) >= 0)
   {
      if (param.m_vstrValue.empty())
         continue;

      if ("SECTOR_PORT" == param.m_strName)
         m_iServerPort = atoi(param.m_vstrValue[0].c_str());
      else if ("SECURITY_SERVER" == param.m_strName)
      {
         char buf[128];
         strncpy(buf, param.m_vstrValue[0].c_str(), 128);

         unsigned int i = 0;
         for (unsigned int n = strlen(buf); i < n; ++ i)
         {
            if (buf[i] == ':')
               break;
         }

         buf[i] = '\0';
         m_strSecServIP = buf;
         m_iSecServPort = atoi(buf + i + 1);
      }
      else if ("MAX_ACTIVE_USER" == param.m_strName)
         m_iMaxActiveUser = atoi(param.m_vstrValue[0].c_str());
      else if ("DATA_DIRECTORY" == param.m_strName)
      {
         m_strHomeDir = param.m_vstrValue[0];
         if (m_strHomeDir.c_str()[m_strHomeDir.length() - 1] != '/')
            m_strHomeDir += "/";
      }
      else if ("REPLICA_NUM" == param.m_strName)
         m_iReplicaNum = atoi(param.m_vstrValue[0].c_str());
      else if ("META_LOC" == param.m_strName)
      {
         if ("MEMORY" == param.m_vstrValue[0])
            m_MetaType = MEMORY;
         else if ("DISK" == param.m_vstrValue[0])
            m_MetaType = DISK;
      }
      else if ("SLAVE_TIMEOUT" == param.m_strName)
      {
         m_iSlaveTimeOut = atoi(param.m_vstrValue[0].c_str());
         if (m_iSlaveTimeOut < 120)
            m_iSlaveTimeOut = 120;
      }
      else if ("SLAVE_MIN_DISK_SPACE" == param.m_strName)
      {
#ifndef WIN32
         m_llSlaveMinDiskSpace = atoll(param.m_vstrValue[0].c_str()) * 1000000;
#else
         m_llSlaveMinDiskSpace = _atoi64(param.m_vstrValue[0].c_str()) * 1000000;
#endif
      }
      else if ("CLIENT_TIMEOUT" == param.m_strName)
      {
         m_iClientTimeOut = atoi(param.m_vstrValue[0].c_str());
      }
      else if ("LOG_LEVEL" == param.m_strName)
      {
         m_iLogLevel = atoi(param.m_vstrValue[0].c_str());
      }
      else
      {
         cerr << "unrecongnized system parameter: " << param.m_strName << endl;
      }
   }

   parser.close();

   return 0;
}
