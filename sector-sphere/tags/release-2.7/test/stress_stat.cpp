// stress testing the master

#include <iostream>

#include "sector.h"
#include "common.h"

using namespace std;

int main()
{
   SNode s;
   Sector client;
   Utility::login(client);
   while (true)
   {
      client.stat("/tmp/testfile", s);
   }

   Utility::logout(client);
   return 0;
}
