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

int download(CFSClient& fsclient, const char* file, const char* dest)
{
   timeval t1, t2;
   gettimeofday(&t1, 0);

   CFileAttr attr;
   if (fsclient.stat(file, attr) < 0)
   {
      cout << "ERROR: cannot locate file " << file << endl;
      return -1;
   }

   long long int size = attr.m_llSize;
   cout << "downloading " << file << " of " << size << " bytes" << endl;

   //CProgressBar bar;
   //bar.init(size);

   CCBFile* fh = fsclient.createFileHandle();
   if (NULL == fh)
      return -1;
   if (fh->open(file) < 0)
   {
      cout << "unable to locate file" << endl;
      return -1;
   }

   string localpath;
   if (dest[strlen(dest) - 1] != '/')
      localpath = string(dest) + string("/") + string(file);
   else
      localpath = string(dest) + string(file);

   bool finish = true;
   if (fh->download(localpath.c_str(), true) < 0)
      finish = false;

   fh->close();
   fsclient.releaseFileHandle(fh);

   if (finish)
   {
      gettimeofday(&t2, 0);
      float throughput = size * 8.0 / 1000000.0 / ((t2.tv_sec - t1.tv_sec) + (t2.tv_usec - t1.tv_usec) / 1000000.0);

      //bar.update(size, throughput);

      cout << "Downloading accomplished! " << "AVG speed " << throughput << " Mb/s." << endl << endl ;
   }
   else
      cout << "Downloading failed! Please retry. " << endl << endl;

   return 1;
}

int main(int argc, char** argv)
{
   vector<string> filelist;
   ifstream src(argv[3]);
   char buf[1024];

   while (!src.eof())
   {
      src.getline(buf, 1024);
      if (0 != strlen(buf))
         filelist.insert(filelist.end(), buf);
   }
   src.close();


   CFSClient fsclient;
   if (-1 == fsclient.connect(argv[1], atoi(argv[2])))
   {
      cout << "unable to connect to the server at " << argv[1] << endl;
      return -1;
   }

   for (vector<string>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      download(fsclient, i->c_str(), argv[4]);

   fsclient.close();

   return 1;
}
