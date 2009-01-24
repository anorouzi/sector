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

   int r = Sector::remove(argv[3]);
   if (r < 0)
      cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

   Sector::logout();
   Sector::close();

   return 1;
}
