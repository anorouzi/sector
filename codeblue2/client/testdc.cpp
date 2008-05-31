#include "dcclient.h"
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   const int fn = 1;

   vector<string> files;
   for (int i = 0; i < fn; ++ i)
   {
      char filename[256];
      sprintf(filename, "test/sort_input.%d.dat", i);
      files.insert(files.end(), filename);
      cout << "INPUT " << filename << endl;
   }

   SphereStream s;
   if (s.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream temp;
   temp.setOutputPath("test", "stream_sort_bucket");
   temp.init(4);

   SphereProcess myproc;

   myproc.loadOperator("./examples/sorthash.so");
   myproc.loadOperator("./examples/sort.so");

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   int n = 2;
   if (myproc.run(s, temp, "sorthash", 1, (char*)&n, sizeof(int)) < 0)
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
         continue;
      }
   }

   gettimeofday(&t, 0);
   cout << "stage 1 accomplished " << t.tv_sec << endl;

   SphereStream output;
   output.init(0);

   if (myproc.run(temp, output, "sort", 0, NULL, 0) < 0)
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
         continue;
      }
   }

   gettimeofday(&t, 0);
   cout << "stage 2 accomplished " << t.tv_sec << endl;

   cout << "SPE COMPLETED " << endl;

   myproc.close();

   Sector::logout();
   Sector::close();

   return 0;
}
