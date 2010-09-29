#include <sector.h>
#include <conf.h>
#include <sys/time.h>
#include <iostream>
#include <utility.h>

using namespace std;

int main(int argc, char** argv)
{
   if (1 != argc)
   {
      cout << "usage: testfs" << endl;
      return 0;
   }

   Sector client;
   if (Utility::login(client) < 0)
      return -1;

   client.rmr("test");
   client.mkdir("test");
   client.remove("/tmp/guide.dat");
   client.remove("/tmp/guide.dat.idx");

   SysStat sys;
   client.sysinfo(sys);
   const int fn = sys.m_llTotalSlaves;

   SectorFile* guide = client.createSectorFile();
   if (guide->open("tmp/guide.dat", SF_MODE::WRITE | SF_MODE::TRUNC) < 0)
   {
      cout << "error to open seed file." << endl;
      Utility::logout(client);
      return -1;
   }
   int32_t* id = new int32_t[fn];
   for (int i = 0; i < fn; ++ i) {id[i] = i;}
   guide->write((char*)id, 4 * fn);
   delete [] id;
   guide->close();

   if (guide->open("tmp/guide.dat.idx", SF_MODE::WRITE | SF_MODE::TRUNC) < 0)
   {
      cout << "error to open seed index." << endl;
      Utility::logout(client);
      return -1;
   }
   int64_t* idx = new int64_t[fn + 1];
   idx[0] = 0;
   for (int i = 1; i <= fn; ++ i) {idx[i] = idx[i - 1] + 4;}
   guide->write((char*)idx, 8 * (fn + 1));
   delete [] idx;
   guide->close();
   client.releaseSectorFile(guide);

   // write files to each node
   vector<string> files;
   files.insert(files.end(), "tmp/guide.dat");

   SphereStream input;
   if (input.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      Utility::logout(client);
      return -1;
   }

   SphereStream output;
   output.init(0);

   SphereProcess* myproc = client.createSphereProcess();
   if (myproc->loadOperator("./funcs/randwriter.so") < 0)
   {
      cout << "unable to load operator. quit\n";
      Utility::logout(client);
      return -1;
   }

   myproc->setMinUnitSize(4);
   myproc->setMaxUnitSize(4);

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   string target = "test/sort_input";
   int result = myproc->run(input, output, "randwriter", -1, target.c_str(), target.length() + 1);
   if (result < 0)
   {
      Utility::print_error(result);
      Utility::logout(client);
      return -1;
   }

   while (true)
   {
      SphereResult* res = NULL;

      if (myproc->read(res) <= 0)
      {
         if (myproc->checkProgress() < 0)
         {
            cerr << "all SPEs failed\n";
            break;
         }

         if (myproc->checkProgress() == 100)
            break;
      }
      else
      {
         delete res;
      }
   }

   myproc->close();
   client.releaseSphereProcess(myproc);

   client.rmr("tmp");

   Utility::logout(client);

   return 0;
}
