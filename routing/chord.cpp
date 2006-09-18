#include <routing.h>

int main(int argc, char** argv)
{
   CRouting chord;

   if (2 == argc)
   {
      chord.start(argv[1]);
   }
   else if (4 == argc)
   {
      chord.join(argv[1], argv[2], atoi(argv[3]));
   }
   else
      return -1;

   while (true)
      sleep(100);

   return 1;
}
