// stress testing the master

#include <sector.h>
#include <iostream>

using namespace std;

int main()
{
   while (true)
   {
      Sector client;
      Utility::login(client);
      Utility::logout(client);
   }

   return 0;
}
