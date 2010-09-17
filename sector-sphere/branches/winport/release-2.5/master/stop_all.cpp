#include <conf.h>
#include <cstdlib>
#include <cstring>
#include <string.h>
#include <string>
#include <fstream>
#include <iostream>

using namespace std;

int main()
{
   cout << "Warning! This command will terminate this master and all slave nodes with extreme prejudice. If you need a graceful shutdown, use ./tools/sector_shutdown.\n";
   cout << "Do you want to continue? Y/N:";
   char answer;
   cin >> answer;
   if ((answer != 'Y') && (answer != 'y'))
   {
      cout << "aborted.\n";
      return -1;
   }

#ifndef WIN32
   system("killall -9 start_master");
#else
   system("taskkill /F /IM start_master.exe");
#endif
   cout << "master node stopped\n";

   string sector_home;
   if (ConfLocation::locate(sector_home) < 0)
   {
      cerr << "no Sector information located; nothing to stop.\n";
      return -1;
   }

   ifstream ifs((sector_home + "/conf/slaves.list").c_str());

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

      //string base = newline;
      //base = base.substr(base.find(' ') + 1, base.length());

      string addr = newline;
      addr = addr.substr(0, addr.find(' '));

      cout << "stopping slave node at " << addr << endl;

      system((string("ssh ") + addr + " killall -9 start_slave &").c_str());
   }

   return 0;
}
