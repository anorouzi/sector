#include <dcclient.h>
#include <util.h>
#include <probot.h>
#include <iostream>

using namespace std;

void help()
{
   cout << "stream -i input [-o output] -c command [-b buckets] [-p parameters] [-f files]" << endl;
   cout << endl;
   cout << "-i: input file or directory" << endl;
   cout << "-o: output file or directory (optional)" << endl;
   cout << "-b: number of buckets (optional)" << endl;
   cout << "-c: command or program" << endl;
   cout << "-p: parameters (optional)" << endl;
   cout << "-f: file to upload to Sector servers (optional)" << endl;
}

int main(int argc, char** argv)
{
   CmdLineParser clp;
   if (clp.parse(argc, argv) <= 0)
   {
      help();
      return 0;
   }
   
   string inpath;
   string outpath;
   string cmd;
   string parameter;
   int bucket;
   string upload;

   for (map<string, string>::const_iterator i = clp.m_mParams.begin(); i != clp.m_mParams.end(); ++ i)
   {
      if (i->first == "i")
         inpath = i->second;
      else if (i->first == "o")
         outpath = i->second;
      else if (i->first == "c")
         cmd = i->second;
      else if (i->first == "p")
         parameter = i->second;
      else if (i->first == "b")
         bucket = atoi(i->second.c_str());
      else if (i->first == "f")
         upload = i->second;
      else
      {
         help();
         return 0;
      }
   }

   if ((inpath.length() == 0) || (cmd.length() == 0))
   {
      help();
      return 0;
   }

   PRobot pr;
   pr.setCmd(cmd);
   pr.setParam(parameter);
   pr.setCmdFlag(upload.length() != 0);
   pr.generate();
   pr.compile();



   Session s;
   s.loadInfo("../../conf/client.conf");

   if (Sector::init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (Sector::login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;

   vector<string> files;
   files.insert(files.end(), inpath);

   SphereStream input;
   if (input.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream output;
   output.setOutputPath(outpath, "stream_result");
   output.init(bucket);

   SphereProcess myproc;

   if (myproc.loadOperator((string(cmd) + ".so").c_str()) < 0)
      return -1;

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   if (myproc.run(input, output, cmd, 0) < 0)
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

         if (myproc.checkProgress() == 100)
            break;
      }

      if (res->m_iDataLen > 0)
      {
         cout << "RESULT " << res->m_strOrigFile << endl;
         cout << res->m_pcData << endl;
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
