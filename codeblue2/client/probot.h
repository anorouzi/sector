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
   Yunhong Gu [gu@lac.uic.edu], last updated 07/16/2009
*****************************************************************************/


#ifndef __SPHERE_PROGRAM_ROBOT_H__
#define __SPHERE_PROGRAM_ROBOT_H__

#include <string>

class PRobot
{
public:
   PRobot();

public:
   void setCmd(const std::string& name);
   void setParam(const std::string& name);
   void setCmdFlag(const bool& local);

public:
   int generate();
   int compile();

private:
   std::string m_strSrc;
   std::string m_strCmd;
   std::string m_strParam;
   bool m_bLocal;
};

#endif
