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
   cout << "start_all [-s slaves.list] [-l slave_screen_log_output]" << endl;
}

int main(int argc, char** argv)
{
   string sector_home;
   if (ConfLocation::locate(sector_home) < 0)
   {
      cerr << "no Sector information located; nothing to start.\n";
      help();
      return -1;
   }

   string cmd = string("nohup " + sector_home + "/master/start_master > /dev/null &");
   system(cmd.c_str());
   cout << "start master ...\n";


   CmdLineParser clp;
   clp.parse(argc, argv);

   string slaves_list = sector_home + "/conf/slaves.list";
   string slave_screen_log = "/dev/null";

   for (map<string, string>::const_iterator i = clp.m_mDFlags.begin(); i != clp.m_mDFlags.end(); ++ i)
   {
      if (i->first == "s")
         slaves_list = i->second;
      else if (i->first == "l")
         slave_screen_log = i->second;
      else
      {
         help();
         return 0;
      }
   }

   ifstream ifs(slaves_list.c_str());
   if (ifs.bad() || ifs.fail())
   {
      cout << "no slave list found at " << slaves_list << endl;
      return -1;
   }

   int count = 0;

   while (!ifs.eof())
   {
      if (++ count == 64)
      {
         // wait a while to avoid too many incoming slaves crashing the master
         // TODO: check number of active slaves so far
         sleep(1);
         count = 0;
      }

      char line[256];
      line[0] = '\0';
      ifs.getline(line, 256);
      if (*line == '\0')
         continue;

      int i = 0;
      int n = strlen(line);
      for (; i < n; ++ i)
      {
         if ((line[i] != ' ') && (line[i] != '\t'))
            break;
      }

      if ((i == n) && (line[i] == '#'))
         continue;

      char newline[256];
      bool blank = false;
      char* p = newline;
      for (; i <= n; ++ i)
      {
         if ((line[i] == ' ') || (line[i] == '\t'))
         {
            if (!blank)
               *p++ = ' ';
            blank = true;
         }
         else
         {
            *p++ = line[i];
            blank = false;
         }
      }

      string base = newline;
      base = base.substr(base.find(' ') + 1, base.length());

      string addr = newline;
      addr = addr.substr(0, addr.find(' '));

      //TODO: source .bash_profile to include more environments variables
      string cmd = (string("ssh ") + addr + " \"" + base + "/slave/start_slave " + base + " &> " + slave_screen_log + "&\" &");
      system(cmd.c_str());

      cout << "start slave at " << addr << endl;
      cout << "CMD: " << cmd << endl;
   }

   return 0;
}
