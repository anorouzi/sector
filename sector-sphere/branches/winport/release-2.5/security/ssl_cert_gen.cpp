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

#include <sys/types.h>
#include <sys/stat.h>
#ifndef WIN32
    #include <unistd.h>
#endif
#include <string>
#include <cstring>
#include <cstdlib>
#include <iostream>

using namespace std;

void gen_cert(const string& name)
{
   string keyname = name + "_node.key";
   string certname = name + "_node.cert";

   system(("openssl genrsa 1024 > " + keyname).c_str());
#ifndef WIN32
   system(("chmod 400 " + keyname).c_str());
#else
   system(("attrib +R " + keyname).c_str());
#endif
   system(("openssl req -new -x509 -nodes -sha1 -days 365 -batch -key " + keyname + " > " + certname).c_str());
}

int main(int argc, char** argv)
{
   if (argc != 2)
   {
      cout << "usage: ssl_cert_gen security OR ssl_cert_gen_master" << endl;
      return -1;
   }

   if (0 == strcmp(argv[1], "security"))
   {
      struct stat s;
      if (stat("../conf/security_node.key", &s) != -1)
      {
         cerr << "Key already exist\n";
         return -1;
      }

      gen_cert("security");
#ifndef WIN32
      system("mv security_node.* ../conf");
      system("rm -f security_node.*");
#else
      system("move /Y security_node.* ../conf");
      system("del /Q security_node.*");
#endif
   }
   else if (0 == strcmp(argv[1], "master"))
   {
      struct stat s;
      if (stat("../conf/master_node.key", &s) != -1)
      {
         cerr << "Key already exist\n";
         return -1;
      }

      gen_cert("master");
#ifndef WIN32
      system("mv master_node.* ../conf");
      system("rm -f master_node.*");
#else
      system("move /Y master_node.* ../conf");
      system("del /Q master_node.*");
#endif
   }
   else
      return -1;

   return 0;
}
