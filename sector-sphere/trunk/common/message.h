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

#ifndef __SECTOR_MESSAGE_H__
#define __SECTOR_MESSAGE_H__

#include "gmp.h"

// All Sector transactions should inherit from this class
// define its specific message structure, and implement
// serialization. GMP will call serilazation methods when
// transfering the message.

// TYPE: Type of control information carried by the message
// KEY: A token to identify the message sender
// SIGNATURE: A digest of the message using the sender's secret key

class SectorMsg: public CUserMessage
{
public:
   SectorMsg() {m_iDataLength = m_iHdrSize;}

   int32_t getType() const;
   void setType(const int32_t& type);
   int32_t getKey() const;
   void setKey(const int32_t& key);
   char* getData() const;
   void setData(const int& offset, const char* data, const int& len);

public:
   static const int m_iHdrSize = 8;
};

#endif
