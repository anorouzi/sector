#include <client.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 3)
   {
      cout << "USAGE: sysinfo <ip> <port>\n";
      return -1;
   }

   Sector::init(argv[1], atoi(argv[2]));

   char password[128];
   cout << "Please input password for root user: ";
   cin >> password;
   if (Sector::login("root", password) < 0)
   {
      cerr << "incorrect password\n";
      return -1;
   }

   SysStat sys;
   Sector::sysinfo(sys);

   sys.print();

   Sector::logout();
   Sector::close();

   return 1;
}
