/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 04/23/2008
*****************************************************************************/

#include "security.h"
#include <iostream>
using namespace std;

int main(int argc, char** argv)
{
   SClient sc;

   sc.init("host.cert");
   sc.connect("ncdm161.lac.uic.edu", 5000);

   sc.sendReq(argv[1], argv[2]);
   int code;
   sc.recvRes(code);

   cout << "RESULT: " << code << endl;

   return 1;
}
