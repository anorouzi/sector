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
   Yunhong Gu, last updated 10/14/2010
*****************************************************************************/

#include <sector.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace std;

void help()
{
   cout << "stop_all [-s slaves_list]" << endl;
}

bool skip(char* line)
{
   if (*line == '\0')
      return true;

   int i = 0;
   int n = strlen(line);
   for (; i < n; ++ i)
   {
      if ((line[i] != ' ') && (line[i] != '\t'))
         break;
   }

   if ((i == n) || (line[i] == '#') || (line[i] == '*'))
      return true;

   return false;
}


int main(int argc, char** argv)
{   string sector_home;
   if (ConfLocation::locate(sector_home) < 0)
   {
      cerr << "no Sector information located; nothing to stop.\n";
      return -1;
   }

   string slaves_list = sector_home + "/conf/slaves.list";

   CmdLineParser clp;
   clp.parse(argc, argv);
   for (map<string, string>::const_iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "s")
         slaves_list = i->second;
      else
      {
         help();
         return 0;
      }
   }

   cout << "This will stop this master and all slave nodes by brutal forces. If you need a graceful shutdown, use ./tools/sector_shutdown.\n";
   cout << "Do you want to continue? Y/N:";
   char answer;
   cin >> answer;
   if ((answer != 'Y') && (answer != 'y'))
   {
      cout << "aborted.\n";
      return -1;
   }

   system("killall -9 start_master");
   cout << "master node stopped\n";

   ifstream ifs(slaves_list.c_str());
   if (ifs.bad() || ifs.fail())
   {
      cerr << "no slave list found!\n";
      return -1;
   }

   while (!ifs.eof())
   {
      char line[256];
      line[0] = '\0';
      ifs.getline(line, 256);

      if (skip(line))
         continue;

      string addr = line;
      addr = addr.substr(0, addr.find(' '));

      cout << "stopping slave node at " << addr << endl;

      system((string("ssh -o StrictHostKeychecking=no ") + addr + " killall -9 start_slave &").c_str());
   }

   return 0;
}
