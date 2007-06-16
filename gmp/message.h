/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Group Messaging Protocol (GMP)

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 01/25/2007
*****************************************************************************/


#ifndef __CB_MESSAGE_H__
#define __CB_MESSAGE_H__

#ifndef WIN32
   #include <sys/types.h>

   #define GMP_API
#else
   #include <util.h>

   #ifdef GMP_EXPORTS
      #define GMP_API __declspec(dllexport)
   #else
      #define GMP_API __declspec(dllimport)
   #endif
#endif

#include <iostream>
using namespace std;

namespace cb
{

class GMP_API CUserMessage
{
friend class CGMP;

public:
   CUserMessage();
   CUserMessage(const int& len);
   CUserMessage(const CUserMessage& msg);
   virtual ~CUserMessage();

public:
   int resize(const int& len);

public:
   char* m_pcBuffer;
   int m_iDataLength;
   int m_iBufLength;
};

class GMP_API CRTMsg: public CUserMessage
{
public:
   int32_t getType() const;
   void setType(const int32_t& type);
   char* getData() const;
   void setData(const int& offset, const char* data, const int& len);

public:
   static const int m_iHdrSize = 4;
};

class GMP_API CCBMsg: public CUserMessage
{
public:
   int32_t getType() const;
   void setType(const int32_t& type);
   char* getData() const;
   void setData(const int& offset, const char* data, const int& len);

public:
   static const int m_iHdrSize = 4;
};

}; // namespace

#endif
