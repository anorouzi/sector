// Adapted from:

/* Copyright (C) 1991, 1995, 1996, 1997 Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

// Win32 compatibility
#ifndef DIRENT_H
#define DIRENT_H

// Including SDKDDKVer.h defines the highest available Windows platform.

// If you wish to build your application for a previous Windows platform, include WinSDKVer.h and
// set the _WIN32_WINNT macro to the platform you wish to support before including SDKDDKVer.h.

#include <SDKDDKVer.h>

#include <stdio.h>
#include <time.h>
#include <sys/types.h>
#include <direct.h>

#ifdef COMMON_EXPORTS
    #define COMMON_API __declspec(dllexport)
#else
    #define COMMON_API __declspec(dllimport)
#endif

extern "C" {

    struct COMMON_API dirstream
    {
      void * fd;		/* File descriptor.  */
      char *data;		/* Directory block.  */
      size_t allocation;	/* Space allocated for the block.  */
      size_t size;		/* Total valid data in the block.  */
      size_t offset;	/* Current offset into the block.  */
      off_t filepos;	/* Position of next entry to read.  */
      char *mask;           /* Initial file mask. */
    };

    struct COMMON_API dirent
    {
      long d_ino;
      off_t d_off;
      unsigned short int d_reclen;
      unsigned char d_type;
      char d_name[FILENAME_MAX+1];
    };

    #define d_fileno d_ino /* Backwards compatibility. */

    /* This is the data type of directory stream objects.
       The actual structure is opaque to users.  */

    typedef struct COMMON_API dirstream DIR;


    COMMON_API DIR * opendir (const char * name);
    COMMON_API struct dirent * readdir (DIR * dir);
    COMMON_API int closedir (DIR * dir);
    COMMON_API void rewinddir (DIR * dir);
    COMMON_API void seekdir (DIR * dir, off_t offset);
    COMMON_API off_t telldir (DIR * dir);
    COMMON_API int dirfd (DIR * dir);

    typedef int (__cdecl *SCANDIR_COMPARE_FUNC)(const void *, const void *);

    COMMON_API int scandir (const char *dir, struct dirent **namelist[],
        int(*filter)(const struct dirent *),
        int(*compar)(const struct dirent **, const struct dirent**) );

    COMMON_API int alphasort(const struct dirent **, const struct dirent**);

    COMMON_API int utimes(const char *filename, const struct timeval times[2]);

    struct COMMON_API utimbuf {
         time_t actime;
         time_t modtime;
    };

    COMMON_API int utime (const char *path, struct utimbuf *buf);
}

#define S_IRWXU 0
inline int __cdecl mkdir(const char * szpath, int /*flags*/) {
    return ::_mkdir (szpath);
}

#define unlink _unlink

#endif // DIRENT_H


