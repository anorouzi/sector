#include <slave.h>
#include <iostream>
using namespace std;

int main(int argc, char** argv)
{
   Slave s;

   int res;

   if (argc > 1)
      s.init(argv[1]);
   else
      s.init();

   if (res < 0)
   {
      cout << "couldn't start Sector server...";

      switch (res)
      {
      case -2:
         cout << "routing layer initialization error." << endl;
         return -2;

      case -3:
      case -4:
         cout << "couldn't initialize local directory." << endl;
         return -3;
      }

      return -1;
   }

   s.run();

   return 1;
}
