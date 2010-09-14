#include <conf.h>
#include <cstdlib>
#include <string.h>
#include <string>
#include <fstream>
#include <iostream>

#ifdef WIN32
    #include <Windows.h>
#endif

using namespace std;

int main()
{
   cout << "starting master ...\n";

   string sector_home;
   if (ConfLocation::locate(sector_home) < 0)
   {
      cerr << "no Sector information located; nothing to start.\n";
      return -1;
   }

#ifndef WIN32
   string cmd = string("nohup " + sector_home + "/master/start_master > /dev/null &");
#else
   // NOTE: there is NO 'nohup' option in start cmd
   // TODO: Add options to run master as a NT service (i.e., use srvany), requires admin rights.
   // or would ssh work?
   string cmd = string("start " + sector_home + "/master/start_master.exe > NULL");
#endif
   system(cmd.c_str());

   ifstream ifs((sector_home + "/conf/slaves.list").c_str());

   if (ifs.bad() || ifs.fail())
   {
      cout << "no slave list found!\n";
      return -1;
   }

   int count = 0;

   while (!ifs.eof())
   {
      if (++ count == 64)
      {
         // wait a while to avoid too many incoming slaves crashing the master
         // TODO: check number of active slaves so far
#ifndef WIN32
         sleep(1);
#else
         ::Sleep(1000);
#endif
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
      system((string("ssh ") + addr + " \"" + base + "/slave/start_slave " + base + " &> /dev/null &\" &").c_str());

      cout << "start slave at " << addr << endl;
   }

   return 0;
}
