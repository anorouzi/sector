#include <server.h>

int main(int argc, char** argv)
{
   cb::Server cba(argv[1]);

   if (argc == 2)
   {
      cba.init();
   }
   else
   {
      cba.init(argv[2], atoi(argv[3]));
   }

   cba.run();

   return 1;
}
