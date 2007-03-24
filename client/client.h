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


#ifndef __CB_CLIENT_H__
#define __CB_CLIENT_H__

#include <gmp.h>
#include <node.h>

namespace cb
{

class Client
{
public:
   Client();
   Client(const int& protocol);
   ~Client();

public:
   int connect(const string& server, const int& port);
   int close();

protected:
   int lookup(string filename, Node* n);

protected:
   string m_strServerHost;
   int m_iServerPort;

   CGMP* m_pGMP;

   int m_iProtocol;     // 1 UDT 2 TCP
};

}; // namespace

#endif
