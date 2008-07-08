#include <fstream>
#include <iostream>

using namespace std;

int main()
{
   system("killall -9 start_master");

   cout << "master node stopped\n";

   ifstream ifs("slaves.list");

   if (ifs.bad() || ifs.fail())
   {
      cout << "no slave list found!\n";
      return -1;
   }

   while (!ifs.eof())
   {
      char line[256];
      line[0] = '\0';
      ifs.getline(line, 256);
      if (strlen(line) == 0)
         continue;

      string addr = line;
      addr = addr.substr(0, addr.find(' '));

      cout << "stoping slave node at " << addr << endl;

      system((string("ssh ") + addr + " killall -9 start_slave &").c_str());
   }

   return 0;
}
