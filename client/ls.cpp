#include <algorithm>
#include <fsclient.h>

using namespace std;

bool operator<(const CIndexInfo& a, const CIndexInfo& b) {
    return a.m_llTimeStamp < b.m_llTimeStamp;
}

int main(int argc, char** argv)
{
   CFSClient fsclient;

   fsclient.connect(argv[1], atoi(argv[2]));

   vector<CIndexInfo> filelist;
   fsclient.ls(filelist);

   sort(filelist.begin(), filelist.end());

   for (vector<CIndexInfo>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      cout << i->m_pcName << " " << i->m_llTimeStamp << endl;

   return 1;
}
