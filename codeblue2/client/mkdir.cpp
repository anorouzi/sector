#include <fsclient.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 4)
   {
      cout << "USAGE: rm <ip> <port> <dir>\n";
      return -1;
   }

   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   Sector::mkdir(argv[3]);

   Sector::logout();
   Sector::close();

   return 1;
}
