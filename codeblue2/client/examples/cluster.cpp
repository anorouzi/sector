#include "dcclient.h"
#include <iostream>
#include <cmath>

using namespace std;
using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

   vector<string> files;
   files.insert(files.end(), "uofc2-1193254200-dump.4.3162.pcap.csv");
   files.insert(files.end(), "uofc2-1193253600-dump.4.3161.pcap.csv");
   files.insert(files.end(), "uofc2-1193253000-dump.4.3160.pcap.csv");
   files.insert(files.end(), "uofc2-1193252400-dump.4.3159.pcap.csv");
   files.insert(files.end(), "uofc2-1193251799-dump.4.3158.pcap.csv");
   files.insert(files.end(), "uofc2-1193251200-dump.4.3157.pcap.csv");
   files.insert(files.end(), "isi-1192886400-dump.19.14671.pcap.csv");
   files.insert(files.end(), "isi-1192885800-dump.19.14670.pcap.csv");
   files.insert(files.end(), "isi-1192885200-dump.19.14669.pcap.csv");
   files.insert(files.end(), "isi-1192884600-dump.19.14668.pcap.csv");
   files.insert(files.end(), "isi-1192884000-dump.19.14667.pcap.csv");
   files.insert(files.end(), "isi-1192883400-dump.19.14666.pcap.csv");
   files.insert(files.end(), "anl2-1193251200-dump.22.18025.pcap.csv");
   files.insert(files.end(), "anl2-1193251800-dump.22.18026.pcap.csv");
   files.insert(files.end(), "anl2-1193252400-dump.22.18027.pcap.csv");
   files.insert(files.end(), "anl2-1193253000-dump.22.18028.pcap.csv");
   files.insert(files.end(), "anl2-1193253600-dump.22.18029.pcap.csv");
   files.insert(files.end(), "anl2-1193254200-dump.22.18030.pcap.csv");

   Stream input;
   if (input.init(files) < 0)
      return -1;

   Stream output;
   if (output.init(0) < 0)
      return -1;


   Process* myproc = Sector::createJob();

   const int nfeat = 8;
   const int nclus = 5;

   double centers[nclus][nfeat]; 

   double ad = 0;
   double newad = 0;

   int vnum[nclus];

   for (int i = 0; i < nclus; ++ i)
   {
      for (int j = 0; j < nfeat; ++ j)
         centers[i][j] = double(i + j + 2) / (nclus + nfeat);

      vnum[i] = 0;
   }

   do
   {
      ad = newad;
      newad = 0;

      if (myproc->run(input, output, "kmeans", 0, (char*)centers, nclus * nfeat * 8) < 0)
      {
         cout << "failed to find any computing resources." << endl;
         return -1;
      }

      for (int i = 0; i < nclus; ++ i)
      {
         for (int j = 0; j < nfeat; ++ j)
            centers[i][j] = 0;
         vnum[i] = 0;
      }

      while (true)
      {
         Result* res = NULL;

         if (-1 == myproc->read(res))
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

         for (int i = 0; i < nclus; ++ i)
         {
            for (int j = 0; j < nfeat; ++ j)
               centers[i][j] += *(double*)(res->m_pcData + 8 * (i * nfeat + j));
         }

         for (int i = 0; i < nclus; ++ i)
            vnum[i] += *(int*)(res->m_pcData + nclus * nfeat * 8 + 4 * i);

         newad += *(double*)(res->m_pcData + nclus * nfeat * 8 + 4 * nclus);

         delete [] res->m_pcData;
         delete res;
         res = NULL;
      }

      for (int i = 0; i < nclus; ++ i)
      {
         for (int j = 0; j < nfeat; ++ j)
         {
            if (0 != vnum[i])
               centers[i][j] /= vnum[i];
         }
      }

      cout << "result " << newad << endl;

      for (int i = 0; i < nclus; ++ i)
      {
         for (int j = 0; j < nfeat; ++ j)
            cout << centers[i][j] << " ";
         cout << endl;
      }

      for (int i = 0; i < nclus; ++ i)
         cout << vnum[i] << " ";
      cout << endl;

   } while (fabs(ad - newad) > 0.01);

   cout << "SPE COMPLETED " << endl;

   myproc->close();
   Sector::releaseJob(myproc);

   Sector::close();

   return 0;
}
