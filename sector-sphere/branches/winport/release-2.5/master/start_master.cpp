#include <master.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   cout << SectorVersion << endl;

   Master m;

   if (m.init() < 0)
      return -1;

   if (argc == 3)
   {
      if (m.join(argv[1], atoi(argv[2])) < 0)
         return -1;
   }

   cout << "Sector master is successfully running now. check the master log at $DATA_DIRECTORY/.log for more details.\n";
   cout << "There is no further screen output from this program.\n";

   m.run();
#ifndef WIN32
   sleep(1);
#else
   ::Sleep(1000);
#endif

   return 1;
}
