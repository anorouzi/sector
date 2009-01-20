#include <fsclient.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 5)
   {
      cout << "USAGE: mv <ip> <port> <src_file/dir> <dst_file/dir>\n";
      return -1;
   }

   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   Sector::move(argv[3], argv[4]);

   Sector::logout();
   Sector::close();

   return 1;
}
