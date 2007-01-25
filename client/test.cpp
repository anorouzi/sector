#include <fsclient.h>

int main(int argc, char** argv)
{
   CFSClient fsclient;

   fsclient.connect(argv[1], atoi(argv[2]));

   CCBFile* f1 = fsclient.createFileHandle();
   if (NULL == f1)
      return -1;

   if (f1->open("test.txt") < 0)
   {
      cout << "error to open file." << endl;
      return -1;
   }

   char buf[1024];
   f1->read(buf, 0, 10);

   string test = "**********Hello World!";

   f1->write(test.c_str(), 20, test.length());

   f1->close();

   f1->open("test.txt");
   int res = f1->read(buf, 0, 10);

   buf[10] = '\0';
   cout << "res = " << res << " " << buf << endl;

   f1->write(test.c_str(), 20, test.length());
   f1->close();

   f1->open("test.txt");
   f1->read(buf, 0, 10);
   f1->write(test.c_str(), 20, test.length());
   f1->close();

   fsclient.releaseFileHandle(f1);

   CFileAttr attr;
   fsclient.stat("test.txt", attr);
   cout << attr.m_pcName << " " << attr.m_pcHost << " " << attr.m_iPort << " " << attr.m_llSize << endl;

   fsclient.stat("rate.txt", attr);
   cout << attr.m_pcName << " " << attr.m_pcHost << " " << attr.m_iPort << " " << attr.m_llSize << endl;

   vector<CIndexInfo> filelist;
   fsclient.ls(filelist);

   cout << endl << "LS " << endl;
   for (vector<CIndexInfo>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      cout << i->m_pcName << "\t" << i->m_TimeStamp.tv_sec << endl;

   return 1;
}
