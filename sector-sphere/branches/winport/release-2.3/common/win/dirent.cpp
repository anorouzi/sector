// dirent.cpp: emulates POSIX directory readin functions: opendir(), readdir(),
//           etc. under Win32   
//

#include <windows.h>
#include <errno.h>
#include <sys/stat.h>
#include <string.h>
#include <malloc.h>
#include <time.h>
#include "dirent.h"



/*
EACCES 
    Permission denied. 
EMFILE 
    Too many file descriptors in use by process. 
ENFILE 
    Too many files are currently open in the system. 
ENOENT 
    Directory does not exist, or name is an empty string. 
ENOMEM 
    Insufficient memory to complete the operation. 
ENOTDIR 
    name is not a directory
*/
COMMON_API DIR * opendir (const char * name)
{
    DWORD fileAttribute = ::GetFileAttributes (name);

    // Check if directory exists.
    if (fileAttribute == INVALID_FILE_ATTRIBUTES) {
        _set_errno (ENOENT);
        return NULL;
    }

    // Check if filename is a valid directory.
    if (!(fileAttribute & FILE_ATTRIBUTE_DIRECTORY)) {
        _set_errno (ENOTDIR);
        return NULL;
    }

  DIR *dir = NULL;
  HANDLE hnd;
  char *file;
  WIN32_FIND_DATA find;

  if (!name || !*name) 
    return NULL;

  size_t name_len = strlen (name);
  if (name_len + 3 > FILENAME_MAX)
      name_len = FILENAME_MAX - 3;

  const size_t fname_size = name_len + 3 + 1;
  file = (char *)malloc (fname_size);
  strcpy_s (file, fname_size, name);

  if (file[name_len - 1] != '/' && file[name_len - 1] != '\\')
    strcat_s (file, fname_size, "/*");
  else
    strcat_s (file, fname_size, "*");
  
  if ((hnd = FindFirstFile (file, &find)) == INVALID_HANDLE_VALUE)
  {
       _set_errno (ENOENT);
      free (file);
      return NULL;
  }

  size_t size = sizeof (DIR);
  dir = (DIR *)malloc (size);
  if (!dir) {
      free (file);
      FindClose (hnd);
       _set_errno (ENOMEM);
      return NULL;
  }

  dir->mask = file;
  dir->fd = hnd;
  dir->data = (char *)malloc (sizeof (WIN32_FIND_DATA));
  dir->allocation = sizeof (WIN32_FIND_DATA);
  dir->size = dir->allocation;
  dir->filepos = 0;
  memcpy (dir->data, &find, sizeof (WIN32_FIND_DATA));

  return dir;
}

struct dirent * readdir (DIR * dir)
{
    if (dir == NULL)
        return 0; // not much to do
    
    static struct dirent entry;
    WIN32_FIND_DATA *find;

    entry.d_ino = 0;
    entry.d_type = 0;
    find = (WIN32_FIND_DATA *) dir->data;

    if (dir->filepos)
    {
      if (!FindNextFile (dir->fd, find))
            return NULL;
    }

    entry.d_off = dir->filepos;
    strncpy_s(entry.d_name, find->cFileName, sizeof (entry.d_name));
    entry.d_reclen = strlen (find->cFileName);
    dir->filepos++;
    return &entry;
}

int closedir (DIR * dir)
{
    if (dir == NULL)
        return 0; // not much to do
    HANDLE hnd = dir->fd;
    free (dir->data);
    free (dir->mask);
    free (dir);
    return FindClose (hnd) ? 0 : -1;
}

void rewinddir (DIR * dir)
{
    if (dir == NULL)
        return; // not much to do

    HANDLE hnd = dir->fd;
    WIN32_FIND_DATA *find = (WIN32_FIND_DATA *) dir->data;

    FindClose (hnd);
    hnd = FindFirstFile (dir->mask, find);
    dir->fd = hnd;
    dir->filepos = 0;
}

void seekdir (DIR * dir, off_t offset)
{
    if (dir == NULL)
        return; // not much to do
    off_t n;

    rewinddir (dir);
    for (n = 0; n < offset; n++)
    {
        if (FindNextFile (dir->fd, (WIN32_FIND_DATA *) dir->data))
            dir->filepos++;
    }
}

off_t telldir (DIR * dir)
{
    if (dir == NULL)
        return 0; // not much to do
    return dir->filepos;
}

int dirfd (DIR * dir)
{
    if (dir == NULL)
        return 0; // not much to do
    return (int)dir->fd;
}


//------------------------------------------------------------------------------------

int alphasort(const struct dirent **a, const struct dirent**b) {
  return strcmp ((*static_cast<const struct dirent * const *>(a))->d_name,
                 (*static_cast<const struct dirent * const *>(b))->d_name);
}

int scandir (const char * dirname, struct dirent **namelist[],
    int(* selector)(const struct dirent *) ,
    int(* comparator)(const struct dirent **, const struct dirent**) )
{
  DIR *dirp = opendir (dirname);

  if (dirp == 0)
    return -1;
  // A sanity check here.  "namelist" had better not be zero.
  else if (namelist == 0)
    return -1;

  struct dirent **vector = 0;
  struct dirent *dp = 0;
  int arena_size = 0;
  int nfiles = 0;
  int fail = 0;

  // @@ This code shoulduse readdir_r() rather than readdir().
  for (dp = readdir (dirp);
       dp != 0;
       dp = readdir (dirp))
    {
      if (selector && (*selector)(dp) == 0)
        continue;

      // If we get here, we have a dirent that the user likes.
      if (nfiles == arena_size)
        {
          struct dirent **newv = 0;
          if (arena_size == 0)
            arena_size = 10;
          else
            arena_size *= 2;

          newv = ( struct dirent **) realloc (vector,
                                              arena_size * sizeof ( struct dirent *));
          if (newv == 0)
            {
              fail = 1;
              break;
            }
          vector = newv;
        }

      struct dirent *newdp = (struct dirent *) malloc (sizeof (struct dirent));

      if (newdp == 0)
        {
          fail = 1;
          break;
        }

      // Don't use memcpy here since d_name is now a pointer
      newdp->d_ino = dp->d_ino;
      newdp->d_off = dp->d_off;
      newdp->d_reclen = dp->d_reclen;
      strcpy_s (newdp->d_name, FILENAME_MAX, dp->d_name);
      vector[nfiles++] = newdp;
    }

  if (fail)
  {
      closedir (dirp);
      while (vector && nfiles-- > 0)
      {
          free (vector[nfiles]);
      }
      free (vector);
      return -1;
  }

  closedir (dirp);

  *namelist = vector;

  if (comparator)
    qsort (*namelist,
            nfiles,
            sizeof (struct dirent *),
            (SCANDIR_COMPARE_FUNC)comparator);

  return nfiles;
}



//-----------------------------------------------------------------------------
#define SECONDS_SINCE_1601	11644473600LL
#define USEC_IN_SEC			1000000LL

//after Microsoft KB167296  
static void UnixTimevalToFileTime(struct timeval t, LPFILETIME pft)
{
	LONGLONG ll;
 
	ll = Int32x32To64(t.tv_sec, USEC_IN_SEC*10) + t.tv_usec*10 + \
        SECONDS_SINCE_1601 * USEC_IN_SEC*10;
    pft->dwLowDateTime = (DWORD)ll;
	pft->dwHighDateTime = ll >> 32;
}

int utimes (const char *filename, const struct timeval times[2])
{
    int res = 0;

	FILETIME LastAccessTime;
	FILETIME LastModificationTime;

	if(times) {
		UnixTimevalToFileTime(times[0], &LastAccessTime);
		UnixTimevalToFileTime(times[1], &LastModificationTime);
	}
	else {
		GetSystemTimeAsFileTime(&LastAccessTime);
		GetSystemTimeAsFileTime(&LastModificationTime);
	}

    /* MSDN suggests using FILE_FLAG_BACKUP_SEMANTICS for accessing
        the times of directories.  FIXME: what about Win95??? */
    /* Note: It's not documented in MSDN that FILE_WRITE_ATTRIBUTES is
        sufficient to change the timestamps... */
    HANDLE hFile = CreateFileA (filename, 
            FILE_WRITE_ATTRIBUTES, 
            FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
            NULL, 
            OPEN_EXISTING, 
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS, 
            NULL);

	if (hFile==INVALID_HANDLE_VALUE) {
/*
      if ((res = GetFileAttributes (filename)) != -1 &&
		(res & FILE_ATTRIBUTE_DIRECTORY))
	    {
	      // What we can do with directories more?
	      return 0;
	    } 
*/

		switch(GetLastError()) {
			case ERROR_FILE_NOT_FOUND:
				errno=ENOENT;
				break;
			case ERROR_PATH_NOT_FOUND:
			case ERROR_INVALID_DRIVE:
				errno=ENOTDIR;
				break;
/*			case ERROR_WRITE_PROTECT:	//CreateFile sets ERROR_ACCESS_DENIED on read-only devices 				errno=EROFS;
				break;*/
			case ERROR_ACCESS_DENIED:
				errno=EACCES;
				break;
			default:
				errno=ENOENT;	//what other errors can occur?
		}
		return -1;
	}

	if (!SetFileTime(hFile, NULL, &LastAccessTime, &LastModificationTime)) {
		//can this happen?
		errno=ENOENT;
		return -1;
	}
	
    CloseHandle(hFile);
	return 0;
}


inline timeval time_t_to_timeval (time_t in)
{
    timeval res;
    res.tv_sec = static_cast<long>(in);
    res.tv_usec = 0;
    return res;
}

/* utime */
int utime (const char *path, struct utimbuf *buf)
{
    struct timeval tmp[2];

    if (buf == 0)
        return utimes (path, 0);

    //printf ("incoming utime act %x", buf->actime);
    tmp[0] = time_t_to_timeval (buf->actime);
    tmp[1] = time_t_to_timeval (buf->modtime);

    return utimes (path, tmp);
}


/*
int main ( int argc, char ** argv )
{
  DIR * dir;
  struct dirent * de;
  char * arg;

  arg = argc > 1 ? argv[1] : ".";

  if (dir = opendir( arg ))
  {
    while (de = readdir( dir ))
    {
      puts( de->d_name );
    }
    closedir( dir );
  }
  return 0;
};

#include <dirent.h>
main(){
    struct dirent **namelist;
    int n;
    n = scandir(".", &namelist, 0, alphasort);
    if (n < 0)
        perror("scandir");
    else {
        while(n--) {
            printf("%s\n", namelist[n]->d_name);
            free(namelist[n]);
        }
        free(namelist);
    }
}
*/


