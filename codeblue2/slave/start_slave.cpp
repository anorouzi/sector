#include <slave.h>

int main(int argc, char** argv)
{
   Slave s;

   int res;

   if (argc > 1)
      res = s.init(argv[1]);
   else
      res = s.init();

   if (res < 0)
      return -1;

   if (s.run() < 0)
      return -1;

   return 1;
}
