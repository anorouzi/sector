#include <fstream>
#include <fsclient.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <string.h>
#include <errno.h>

using namespace std;

class CProgressBar
{
public:
   CProgressBar(): m_pcBarData(NULL) {}
   ~CProgressBar() {delete [] m_pcBarData;}

   void init(const long long int& total)
   {
      m_iTermWidth = getTermWidth();
      m_iBarWidth = m_iTermWidth - 40;

      m_pcBarData = new char[m_iTermWidth + 1];

      m_pcBarData[0] = '|';
      for (int i = 1; i < m_iBarWidth; ++ i)
         m_pcBarData[i] = ' ';
      m_pcBarData[m_iBarWidth] = '|';
      m_pcBarData[m_iTermWidth - 1] = '\0';

      m_llTotal = total;

      sprintf(m_pcBarData + m_iBarWidth + 1, "%12lld bytes   ", 0LL);
      sprintf(m_pcBarData + m_iBarWidth + 22, "%#9.3f mb/s    ", 0.0);

      cout << m_pcBarData;
      cout.flush();
   }

   void update(const long long int& progress, const float& throughput)
   {
      int n = int((m_iBarWidth - 2.0) * (double(progress) / m_llTotal));
      if (0 == n)
         n = 1;

      for (int i = 0; i < n; ++ i)
         m_pcBarData[i + 1] = '*';

      sprintf(m_pcBarData + m_iBarWidth + 1, "%12lld bytes   ", progress);
      sprintf(m_pcBarData + m_iBarWidth + 22, "%#9.3f mb/s    ", throughput);

      for (int i = strlen(m_pcBarData); i > 0; -- i)
         cout << "\b";
      cout << m_pcBarData;
      cout.flush();
   }

private:
   int m_iTermWidth;
   int m_iBarWidth;
   char* m_pcBarData;

   long long int m_llTotal;

private:
   int getTermWidth()
   {
      winsize ws;

      if(isatty(fileno(stdout)) == 0)
         return 80;

      if(ioctl(fileno(stdout), TIOCGWINSZ, &ws) < 0)
         return 80;

      return ws.ws_col;
   }
};

int upload(CFSClient& fsclient, const char* file)
{
   timeval t1, t2;
   gettimeofday(&t1, 0);

   ifstream ifs(file);
   ifs.seekg(0, ios::end);
   long long int size = ifs.tellg();
   ifs.seekg(0);
   cout << "uploading " << file << " of " << size << " bytes" << endl;

   //CProgressBar bar;
   //bar.init(size);

   CCBFile* fh = fsclient.createFileHandle();
   if (NULL == fh)
      return -1;

   char* rname = (char*)file;
   for (int i = strlen(file); i >= 0; -- i)
   {
      if ('/' == *rname)
      {
         rname = (char*)file + i + 1;
         break;
      }
   }

   cout << "rname " << rname << endl;

   char cert[1024];
   cert[0] = '\0';
   fh->open(rname, 2, cert);

   if (0 != strlen(cert))
   {
      cout << "got a cert " << cert << endl;

      ofstream ofs((string(rname) + ".cert").c_str());
      ofs << cert << endl;
      ofs.close();
   }

   bool finish = true;
   if (fh->upload(file) < 0)
      finish = false;

   fh->close();
   fsclient.releaseFileHandle(fh);

   if (finish)
   {
      gettimeofday(&t2, 0);
      float throughput = size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

      //bar.update(size, throughput);

      cout << "Uploading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
   }
   else
      cout << "Uploading failed! Please retry. " << endl << endl;

   return 1;
}

int main(int argc, char** argv)
{
   if (4 != argc)
   {
      cout << "usage: upload <filename>" << endl;
      return 0;
   }

   CFSClient fsclient;
   fsclient.connect(argv[1], atoi(argv[2]));

   upload(fsclient, argv[3]);

   fsclient.close();

   return 1;
}
