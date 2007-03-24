/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#ifndef __CONF_H__
#define __CONF_H__

#include <string>
#include <fstream>

using namespace std;

namespace cb
{

struct Param
{
   string m_strName;
   string m_strValue;
};

class ConfParser
{
public:
   int init(string path);
   void close();
   int getNextParam(Param& param);

private:
   char* getToken(char* str, string& token);

private:
   ifstream m_ConfFile;
};

class SECTORParam
{
public:
   int init(const string& path);

public:
   string m_strDataDir;		// DATADIR
   int m_iSECTORPort;		// SECTOR_PORT
   int m_iRouterPort;		// ROUTER_PORT
   int m_iDataPort;		// DATA_PORT
};

}; // namespace

#endif
