/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 01/05/2011
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

   if ((i == n) || (line[i] == '#'))
      return true;

   return false;
}

int parse(char* line, string& addr, string& base, string& param)
{
   //FORMAT: addr(username@IP) base [param]

   addr.clear();
   base.clear();
   param.clear();

   char* start = line;

   // skip all blanks and TABs
   while ((*start == ' ') || (*start == '\t'))
      ++ start;
   if (*start == '\0')
      return -1;

   char* end = start;
   while ((*end != ' ') && (*end != '\t') && (*end != '\0'))
      ++ end;
   if (*end == '\0')
      return -1;

   char orig = *end;
   *end = '\0';
   addr = start;
   *end = orig;


   // skip all blanks and TABs
   start = end;
   while ((*start == ' ') || (*start == '\t'))
      ++ start;
   if (*start == '\0')
      return -1;

   end = start;
   while ((*end != ' ') && (*end != '\t') && (*end != '\0'))
      ++ end;

   orig = *end;
   *end = '\0';
   base = start;
   *end = orig;


   // skip all blanks and TABs
   start = end;
   while ((*start == ' ') || (*start == '\t'))
      ++ start;

   // parameter is optional
   if (*start == '\0')
      return 0;

   param = start;

   return 0;
}

int parse(char* line, string& key, string& val)
{
   //FORMAT:  *KEY=VAL

   char* start = line + 1;
   while (*line != '=')
   {
      if (*line == '\0')
         return -1;
      line ++;
   }
   *line = '\0';

   key = start;

   val = line + 1;

   return 0;
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

   // starting master
   string cmd = string("nohup " + sector_home + "/master/start_master > /dev/null &");
   system(cmd.c_str());
   cout << "start master ...\n";

   // starting slaves on the slave list
   ifstream ifs(slaves_list.c_str());
   if (ifs.bad() || ifs.fail())
   {
      cout << "no slave list found at " << slaves_list << endl;
      return -1;
   }

   int count = 0;
   string mh, mp, log, h, ds;

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

      if (skip(line))
         continue;

      if (*line == '*')
      {
         // global configuration for slaves
         string key, val;
         if (parse(line, key, val) == 0)
         {
            if ("DATA_DIRECTORY" == key)
               h = val;
            else if ("LOG_LEVEL" == key)
               log = val;
            else if ("MASTER_ADDRESS" == key)
            {
               mh = val.substr(0, val.find(':'));
               mp = val.substr(mh.length() + 1, val.length() - mh.length() - 1);
            }
            else if ("MAX_DATA_SIZE" == key)
               ds = val;
            else
               cout << "WARNING: unrecognized option (ignored): " << line << endl;
         }

         continue;
      }

      string addr, base, param;
      if (parse(line, addr, base, param) < 0)
      {
         cout << "WARNING: incorrect slave line format (skipped): " << line << endl;
         continue;
      }

      string global_conf = "";
      if (!mh.empty())
         global_conf += string(" -mh ") + mh;
      if (!mp.empty())
         global_conf += string(" -mp ") + mp;
      if (!h.empty())
         global_conf += string(" -h ") + h;
      if (!log.empty())
         global_conf += string(" -log ") + log;
      if (!ds.empty())
         global_conf += string(" -ds ") + ds;

      string start_slave = base + "/slave/start_slave";

      // slave specific config will overwrite global config; these will overwrite local config, if exists
      param = global_conf + " " + param;

      //TODO: source .bash_profile on slave node to include more environments variables
      string cmd = (string("ssh -o StrictHostKeychecking=no ") + addr + " \"" + start_slave + " " + base + " " + param + " &> " + slave_screen_log + "&\" &");
      system(cmd.c_str());

      cout << "start slave at " << addr << endl;
      cout << "CMD: " << cmd << endl;
   }

   return 0;
}
