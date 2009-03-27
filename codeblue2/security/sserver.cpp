/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 03/08/2009
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

   if (ss.init(port, "../conf/security_node.cert", "../conf/security_node.key") < 0)
   {
      cerr << "failed to initialize security server at port " << port << endl;
      cerr << "Secuirty server failed to start. Please fix the problem.\n";
      return -1;
   }

   if (ss.loadACL("../conf/slave_acl.conf") < 0)
   {
      cerr << "WARNING: failed to read slave ACL configuration file slave_acl.conf in the current directory. No slaves would be able to join.\n";
      cerr << "Secuirty server failed to start. Please fix the problem.\n";
      return -1;
   }

   if (ss.loadShadowFile("../conf/users") < 0)
   {
      cerr << "WARNING: no users account initialized.\n";
      cerr << "Secuirty server failed to start. Please fix the problem.\n";
      return -1;
   }

   cout << "Sector Security server running at port " << port << endl << endl;
   cout << "The server is started successfully; there is no further output from this program. Please do not shutdown the security server; otherwise no client may be able to login. If the server is down for any reason, you can restart it without restarting the masters and the slaves.\n";

   ss.run();

   return 1;
}
