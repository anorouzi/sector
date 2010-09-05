// tests.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <malloc.h>
#include "dirent.h"


int main(int argc, char * argv[])
{
    //const struct timeval times[2]
    utimes("ReadMe.txt", NULL);
    utimes("./", NULL);

    // scandir
    struct dirent **namelist;
    int n = scandir(".", &namelist, 0, alphasort);
    if (n < 0)
        perror("scandir");
    else {
        while(n--) {
            printf("%s\n", namelist[n]->d_name);
            free(namelist[n]);
        }
        free(namelist);
    }

	return 0;
}

#ifdef NOT_USED
  int file_descriptor;
  char fn[]="utime.file";
  struct utimbuf ubuf;
  struct stat info;

  if ((file_descriptor = creat(fn, S_IWUSR)) < 0)
    perror("creat() error");
  else {
    close(file_descriptor);
    puts("before utime()");
    stat(fn,&info);
    printf("  utime.file modification time is %ld\n",
           info.st_mtime);
    ubuf.modtime = 0;       /* set modification time to Epoch */
    time(&ubuf.actime);
    if (utime(fn, &ubuf) != 0)
      perror("utime() error");
    else {
      puts("after utime()");
      stat(fn,&info);
      printf("  utime.file modification time is %ld\n",
             info.st_mtime);
    }
    unlink(fn);
  }
#endif

