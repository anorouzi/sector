#include "fsclient.h"
#include "dcclient.h"
#include <iostream>

using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

   vector<string> pcaps;
   pcaps.insert(pcaps.begin(), "uofc2-1185190800-dump.7.3131.pcap.gz");

   Stream input;
   input.init(pcaps);

   Stream output;
   output.init(0);

   Process* myproc = Sector::createJob();

   if (myproc->run(input, output, "myAngle", 0) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }


   File* f = Sector::createFileHandle();
   if (NULL == f)
      return -1;

   while (true)
   {
      Result* res;

      if (-1 == myproc->read(res, true))
      {
         if (myproc->checkProgress() == -1)
         {
            cerr << "all SPEs failed\n";
            break;
         }

         if (myproc->checkProgress() == 100)
            break;
         continue;
      }

      cout << "got res " << res->m_strOrigFile << endl;

      string f1 = res->m_strOrigFile + ".output.csv";
      char src[68];
      strcpy(src, res->m_strIP.c_str());
      *(int*)(src + 64) = res->m_iPort;
      f->open(f1.c_str(), 1, NULL, src, 1);
      f->download("./angle.output.1");
      f->close();
      // read a result, do sth!!
   }

   Sector::releaseFileHandle(f);

   cout << "SPE COMPLETED " << endl;

   myproc->close();
   Sector::releaseJob(myproc);

   Sector::close();

   return 0;
}
