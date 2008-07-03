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
   Yunhong Gu [gu@lac.uic.edu], last updated 07/02/2008
*****************************************************************************/

#include "security.h"
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   SServer ss;

   int port = 5000;
   if (argc == 2)
      port = atoi(argv[1]);

   if (ss.init(port, "host.cert", "host.key") < 0)
   {
      cerr << "failed to initialize security server at port " << port << endl;
      return -1;
   }

   if (ss.loadACL("slave_acl.conf") < 0)
   {
      cerr << "WARNING: failed to read slave ACL configuration file slave_acl.conf in the current directory. No slaves would be able to join.\n";
   }

   if (ss.loadShadowFile("users") < 0)
   {
      cerr << "WARNING: no users account initialized.\n";
   }

   cout << "Sector Security server running at port " << port << endl;

   ss.run();

   return 1;
}
