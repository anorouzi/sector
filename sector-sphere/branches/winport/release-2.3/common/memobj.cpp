/*****************************************************************************
Copyright (c) 2005 - 2009, The Board of Trustees of the University of Illinois.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 12/12/2009
*****************************************************************************/

#include "meta.h"
#include "common.h"
#include "sphere.h"

using namespace std;

MOMgmt::MOMgmt()
{
}

MOMgmt::~MOMgmt()
{
}

int MOMgmt::add(const string& name, void* loc, const string& user)
{
   CMutexGuard molock(m_MOLock);

   string revised_name = Metadata::revisePath(name);

   if ((revised_name.length() < 7) || (revised_name.substr(0, 7) != "/memory"))
   {
      // all in-memory objects must be named in the directory of "/memory"
      return -1;
   }

   map<string, MemObj>::iterator i = m_mObjects.find(revised_name);

   if (i == m_mObjects.end())
   {
      MemObj object;
      object.m_strName = revised_name;
      object.m_pLoc = loc;
      object.m_strUser = user;
      object.m_llCreationTime = object.m_llLastRefTime = CTimer::getTime();

      m_mObjects[revised_name] = object;

      m_vTBA.push_back(object);

      return 0;
   }

   if (i->second.m_pLoc != loc)
      return -1;

   i->second.m_llLastRefTime = CTimer::getTime();
   return 0;
}

void* MOMgmt::retrieve(const string& name)
{
   CMutexGuard molock(m_MOLock);

   string revised_name = Metadata::revisePath(name);

   map<string, MemObj>::iterator i = m_mObjects.find(revised_name);

   if (i == m_mObjects.end())
      return NULL;

   i->second.m_llLastRefTime = CTimer::getTime();
   return i->second.m_pLoc;
}

int MOMgmt::remove(const string& name)
{
   CMutexGuard molock(m_MOLock);

   string revised_name = Metadata::revisePath(name);

   map<string, MemObj>::iterator i = m_mObjects.find(revised_name);

   if (i == m_mObjects.end())
      return -1;

   m_vTBD.push_back(i->first);

   m_mObjects.erase(i);
   return 0;
}

int MOMgmt::update(vector<MemObj>& tba, vector<string>& tbd)
{
   CMutexGuard molock(m_MOLock);

   tba = m_vTBA;
   m_vTBA.clear();
   tbd = m_vTBD;
   m_vTBD.clear();

   return tba.size() + tbd.size();
}
