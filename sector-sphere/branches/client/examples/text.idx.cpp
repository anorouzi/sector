#include <dcclient.h>
#include <util.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 2)
   {
      cerr << "usage: text.idx text_file_name" << endl;
      return -1;
   }

   Session s;
   s.loadInfo("../../conf/client.conf");

   if (Sector::init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (Sector::login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;

   vector<string> files;
   files.insert(files.end(), argv[1]);

   SphereStream input;
   if (input.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream output;
   output.init(0);

   SphereProcess myproc;

   if (myproc.loadOperator("./funcs/gen_idx.so") < 0)
      return -1;

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   if (myproc.run(input, output, "gen_idx", 0) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);
   t2 = t1;
   while (true)
   {
      SphereResult* res;

      if (myproc.read(res) < 0)
      {
         if (myproc.checkProgress() < 0)
         {
            cerr << "all SPEs failed\n";
            break;
         }
cout << "TEST " << myproc.checkProgress() << endl;
         if (myproc.checkProgress() == 100)
            break;
      }

      gettimeofday(&t2, 0);
      if (t2.tv_sec - t1.tv_sec > 60)
      {
         cout << "PROGRESS: " << myproc.checkProgress() << "%" << endl;
         t1 = t2;
      }
   }

   gettimeofday(&t, 0);
   cout << "mission accomplished " << t.tv_sec << endl;

   myproc.close();

   Sector::logout();
   Sector::close();

   return 0;
}
