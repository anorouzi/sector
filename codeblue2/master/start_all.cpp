#include <fstream>
#include <iostream>

using namespace std;

int main()
{
   system("nohup ./start_master > /dev/null &");
   cout << "start master ...\n";

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

      string base = line;
      base = base.substr(base.find(' ') + 1, base.length());

      string addr = line;
      addr = addr.substr(0, addr.find(' '));

      system((string("ssh ") + addr + " \"" + base + "/start_slave " + base + " &> /dev/null &\"").c_str());

      cout << "start slave at " << addr << endl;
   }

   return 0;
}
