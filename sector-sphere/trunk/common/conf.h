/*****************************************************************************
Copyright 2011 VeryCloud LLC.

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
   Yunhong Gu, last updated 05/18/2011
*****************************************************************************/


#ifndef __SECTOR_CONF_H__
#define __SECTOR_CONF_H__

class ConfParser
{
public:
   int init(const std::string& path);
   void close();
   int getNextParam(Param& param);

private:
   char* getToken(char* str, std::string& token);

private:
   std::ifstream m_ConfFile;
   std::vector<std::string> m_vstrLines;
   std::vector<std::string>::iterator m_ptrLine;
   int m_iLineCount;
};

#endif
