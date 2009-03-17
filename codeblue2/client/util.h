/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/


This file is part of Sector Client.

The Sector Client is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

The Sector Client is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 01/22/2009
*****************************************************************************/


#ifndef __SECTOR_UTIL_H__
#define __SECTOR_UTIL_H__

#include <conf.h>
#include <string>

class WildCard
{
public:
   static bool isWildCard(const std::string& path);
   static bool match(const std::string& card, const std::string& path);
};

class Session
{
public:   
   int loadInfo(const std::string& conf);

public:
   ClientConf m_ClientConf;
};

#endif
