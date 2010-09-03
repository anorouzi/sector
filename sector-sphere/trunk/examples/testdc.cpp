#include <sector.h>
#include <conf.h>
#include <sys/time.h>
#include <iostream>
#include <cmath>

using namespace std;

void print_error(int code)
{
   cerr << "ERROR: " << code << " " << SectorError::getErrorMsg(code) << endl;
}

int main(int argc, char** argv)
{
   if (1 != argc)
   {
      cout << "usage: testdc" << endl;
      return 0;
   }

   Sector client;

   Session se;
   se.loadInfo("../conf/client.conf");

   int result = 0;
   if ((result = client.init(se.m_ClientConf.m_strMasterIP, se.m_ClientConf.m_iMasterPort)) < 0)
   {
      print_error(result);
      return -1;
   }
   if ((result = client.login(se.m_ClientConf.m_strUserName, se.m_ClientConf.m_strPassword, se.m_ClientConf.m_strCertificate.c_str())) < 0)
   {
      print_error(result);
      return -1;
   }

   // remove result of last run
   client.rmr("/test/sorted");

   SysStat sys;
   client.sysinfo(sys);
   const int fn = sys.m_llTotalSlaves;
   const int32_t N = (int)log2(fn * 50.0f);
   const int rn = (int)pow(2.0f, N);

   vector<string> files;
   files.push_back("test");

   SphereStream s;
   if (s.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream temp;
   temp.setOutputPath("test/sorted", "stream_sort_bucket");
   temp.init(rn);

   SphereProcess* myproc = client.createSphereProcess();

   if (myproc->loadOperator("./funcs/sorthash.so") < 0)
   {
      cout << "no sorthash.so found\n";
      return -1;
   }
   if (myproc->loadOperator("./funcs/sort.so") < 0)
   {
      cout << "no sort.so found\n";
      return -1;
   }

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   result = myproc->run(s, temp, "sorthash", 1, (char*)&N, 4);
   if (result < 0)
   {
      print_error(result);
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "stage 1 accomplished " << t.tv_sec << endl;

   SphereStream output;
   output.init(0);
   myproc->setProcNumPerNode(2);

   result = myproc->run(temp, output, "sort", 0, NULL, 0);
   if (result < 0)
   {
      print_error(result);
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "stage 2 accomplished " << t.tv_sec << endl;

   cout << "SPE COMPLETED " << endl;

   myproc->close();
   client.releaseSphereProcess(myproc);

   client.logout();
   client.close();

   return 0;
}
