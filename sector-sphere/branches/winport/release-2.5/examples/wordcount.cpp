#include <iostream>

#include "sector.h"
#include "conf.h"
#include "common.h"

using namespace std;

void print_error(int code)
{
   cerr << "ERROR: " << code << " " << SectorError::getErrorMsg(code) << endl;
}

int main(int argc, char** argv)
{
   if (1 != argc)
   {
      cout << "usage: wordcount" << endl;
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

   vector<string> files;
   files.insert(files.end(), "/html");

   SphereStream s;
   if (s.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream temp;
   temp.setOutputPath("/wordcount", "word_bucket");
   temp.init(256);

   SphereProcess* myproc = client.createSphereProcess();

   if (myproc->loadOperator("./funcs/wordbucket" SECTOR_DYNLIB_EXT) < 0)
   {
      cout << "cannot find workbucket.so\n";
      return -1;
   }

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   result = myproc->run(s, temp, "wordbucket", 0);
   if (result < 0)
   {
      print_error(result);
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "stage 1 accomplished " << t.tv_sec << endl;

   for (vector<string>::iterator i = temp.m_vFiles.begin(); i != temp.m_vFiles.end(); ++ i)
      cout << *i << endl;

   for (vector<int64_t>::iterator i = temp.m_vSize.begin(); i != temp.m_vSize.end(); ++ i)
      cout << *i << endl;

/*
   //NOT FINISHED. PROCESS EACH BUCKET AND GENERATE INDEX

   SphereStream output;
   output.init(0);
   myproc->setProcNumPerNode(2);
   if (myproc->run(temp, output, "index", 0, NULL, 0) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }

   myproc->waitForCompletion();

   gettimeofday(&t, 0);
   cout << "stage 2 accomplished " << t.tv_sec << endl;
*/
   cout << "SPE COMPLETED " << endl;

   myproc->close();
   client.releaseSphereProcess(myproc);

   client.logout();
   client.close();

   return 0;
}
