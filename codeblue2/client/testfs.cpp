#include <fsclient.h>
#include <dcclient.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (3 != argc)
   {
      cout << "usage: testfs <ip> <port>" << endl;
      return 0;
   }

   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   Sector::remove("test");
   Sector::mkdir("test");

   SysStat sys;
   Sector::sysinfo(sys);
   const int fn = sys.m_llTotalSlaves;

   SectorFile guide;
   if (guide.open("test/guide.dat", 2) < 0)
   {
      cout << "error to open file." << endl;
      return -1;
   }
   int32_t* id = new int32_t[fn];
   for (int i = 0; i < fn; ++ i) {id[i] = i;}
   guide.write((char*)id, 0, 4 * fn);
   delete [] id;
   guide.close();

   if (guide.open("test/guide.dat.idx", 2) < 0)
   {
      cout << "error to open file." << endl;
      return -1;
   }
   int64_t* idx = new int64_t[fn + 1];
   idx[0] = 0;
   for (int i = 1; i <= fn; ++ i) {idx[i] = idx[i - 1] + 4;}
   guide.write((char*)idx, 0, 8 * (fn + 1));
   delete [] idx;
   guide.close();

   // write files to each node
   vector<string> files;
   files.insert(files.end(), "test/guide.dat");

   SphereStream input;
   if (input.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream output;
   output.init(0);

   SphereProcess myproc;
   if (myproc.loadOperator("./examples/randwriter.so") < 0)
      return -1;

   myproc.setMinUnitSize(4);
   myproc.setMaxUnitSize(4);

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   string target = "test/sort_input";
   if (myproc.run(input, output, "randwriter", -1, target.c_str(), target.length() + 1) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }

   while (true)
   {
      SphereResult* res;

      if (-1 == myproc.read(res))
      {
         if (myproc.checkProgress() == -1)
         {
            cerr << "all SPEs failed\n";
            break;
         }

         if (myproc.checkProgress() == 100)
            break;
      }
   }

   myproc.close();

   Sector::logout();
   Sector::close();

   return 1;
}
