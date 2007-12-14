#include <server.h>

int main(int argc, char** argv)
{
   cb::Server cba(argv[1]);

   int res = 0;
   if (argc == 2)
      res = cba.init();
   else
      res = cba.init(argv[2], atoi(argv[3]));

   if (res < 0)
   {
      cout << "couldn't start Sector server...";

      switch (res)
      {
      case -1:
         if (argc > 2)
            cout << "didn't find existing server on " << argv[2] << endl;
         return -1;

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

   cba.run();

   return 1;
}
