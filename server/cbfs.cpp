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
      if (cba.init(argv[2], atoi(argv[3])) < 0)
      {
         cout << "didn't find existing server on " << argv[2] << endl;
         return -1;
      }
   }

   cba.run();

   return 1;
}
