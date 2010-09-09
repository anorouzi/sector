#include <slave.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   cout << SectorVersion << endl;

   Slave s;

   int res;

   if (argc > 1)
      res = s.init(argv[1]);
   else
      res = s.init();

   if (res < 0)
      return -1;

   if (s.connect() < 0)
      return -1;

   s.run();

   s.close();

   return 1;
}
