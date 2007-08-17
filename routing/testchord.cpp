#include <chord.h>

int main(int argc, char** argv)
{
   cb::Chord chord;

   if (3 == argc)
   {
      chord.start(argv[1], atoi(argv[2]));
   }
   else if (5 == argc)
   {
      chord.join(argv[1], argv[2], atoi(argv[3]), atoi(argv[4]));
   }
   else
      return -1;

   while (true)
      sleep(100);

   return 1;
}
