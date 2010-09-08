#ifndef STATFS_H
#define STATFS_H


// Adapted from:

/* Definition of `struct statfsx', information about a filesystem.
   Copyright (C) 1996, 1997 Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public License as
   published by the Free Software Foundation; either version 2 of the
   License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.

   You should have received a copy of the GNU Library General Public
   License along with the GNU C Library; see the file COPYING.LIB.  If not,
   write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330,
   Boston, MA 02111-1307, USA.  */

#include <sys/types.h>

#ifdef WIN32
    #include <string>
    #include <algorithm>

    #ifdef COMMON_EXPORTS
        #define COMMON_API __declspec(dllexport)
    #else
        #define COMMON_API __declspec(dllimport)
    #endif
#endif

#define MFSNAMELEN  16   /* length of fs type name, including null */
#define   MNAMELEN  90   /* length of buffer for returned name */

#define __USE_FILE_OFFSET64
#define __USE_LARGEFILE64

// taken from gnu sys/types.h

typedef unsigned int __uid_t;
typedef struct { int __val[2]; } __fsid_t;
typedef __uid_t uid_t;
typedef __fsid_t fsid_t;

typedef unsigned long int __fsblkcnt_t;
typedef unsigned long int __fsfilcnt_t;
typedef __fsfilcnt_t fsfilcnt_t;

typedef unsigned long long int __fsblkcnt64_t;
typedef unsigned long long int __fsfilcnt64_t;

struct statfsx
  {
    unsigned int f_type;           /* type of filesystem (see fsinfo.h) */
    unsigned int f_bsize;               /* file system block size */    
    unsigned long int f_frsize;    /* fragment size: fundamental filesystem block */
    unsigned long int f_iosize;    /* optimal transfer block size */
#ifndef __USE_FILE_OFFSET64
    __fsblkcnt_t f_blocks;              /* total number of blocks on file system
in units of f_frsize */             
    __fsblkcnt_t f_bfree;               /* total number of free blocks */      
                                     
    __fsblkcnt_t f_bavail;              /* number of free blocks available to non-privileged
process */              
    __fsfilcnt_t f_files;          /* total number of file serial numbers */
    __fsfilcnt_t f_ffree;          /* total number of free file serial numbers
*/
    __fsfilcnt_t f_favail;         /* number of file serial numbers available
to non-privileged process */
#else                                        
    __fsblkcnt64_t f_blocks;       /* total number of blocks on file system
in units of f_frsize */
    __fsblkcnt64_t f_bfree;             /* total number of free blocks */    
                          
    __fsblkcnt64_t f_bavail;       /* number of free blocks available to non-privileged
process */ 
    __fsfilcnt64_t f_files;        /* total number of file serial numbers */
                             
    __fsfilcnt64_t f_ffree;        /* total number of free file serial numbers
*/                         
    __fsfilcnt64_t f_favail;  /* number of file serial numbers available to
non-privileged process */
#endif
    __fsid_t f_fsid;                    /* file system id */
    __uid_t     f_owner;       /* user that mounted the filesystem */
    unsigned long int f_flag; /* bit mask of f_flag values */
    char f_fstypename[MFSNAMELEN]; /* fs type name */
    char f_mntonname[MNAMELEN];   /* directory on which mounted */
    char f_mntfromname[MNAMELEN];/* mounted filesystem */
    unsigned int f_namelen;             /* maximum filename length */
  };

#ifdef __USE_LARGEFILE64
struct statfsx64
  {
    unsigned int f_type;           /* type of filesystem (see fsinfo.h) */
    unsigned int f_bsize;               /* file system block size */ 
    unsigned long int f_frsize;    /* fragment size: fundamental filesystem block */
    unsigned long int f_iosize;    /* optimal transfer block size */
    __fsblkcnt64_t f_blocks;       /* total number of blocks on file system
in units of f_frsize */
    __fsblkcnt64_t f_bfree;             /* total number of free blocks */    
                          
    __fsblkcnt64_t f_bavail;       /* number of free blocks available to non-privileged
process */ 
    __fsblkcnt64_t f_files;             /* total number of file serial numbers
*/                       
    __fsblkcnt64_t f_ffree;             /* total number of free file serial numbers
*/                  
    __fsfilcnt_t f_favail;         /* number of file serial numbers available
to non-privileged process */
    __fsid_t f_fsid;                    /* file system id */         
    __uid_t     f_owner;       /* user that mounted the filesystem */
    unsigned long int f_flag; /* bit mask of f_flag values */
    char f_fstypename[MFSNAMELEN]; /* fs type name */
    char f_mntonname[MNAMELEN];   /* directory on which mounted */
    char f_mntfromname[MNAMELEN];/* mounted filesystem */
    unsigned int f_namelen;             /* maximum filename length */
  };
#endif

/* Tell code we have this member.  */
#define _STATFS_F_NAMELEN

struct statfs {
   long      f_type;
   long      f_bsize;
   long      f_frsize;   /* Fragment size - unsupported */
   long      f_blocks;
   long      f_bfree;
   long      f_files;
   long      f_ffree;

   /* Linux specials */
   long      f_bavail;
   __fsid_t  f_fsid;
   long      f_namelen;
   long      f_spare[6];
};


struct statfs64 {         /* Same as struct statfs */
   long      f_type;
   long      f_bsize;
   long      f_frsize;   /* Fragment size - unsupported */
   long      f_blocks;
   long      f_bfree;
   long      f_files;
   long      f_ffree;

   /* Linux specials */
   long      f_bavail;
   __fsid_t  f_fsid;
   long      f_namelen;
   long      f_spare[6];
};

#define ST_RDONLY 0x01 /* read-only file system */
#define ST_NOSUID 0x02 /* does not support setuid/setgid */
#define ST_NOTRUNC 0x04 /* does not truncate long file names */

extern "C" COMMON_API int __statfs (const char *file, struct statfs *buf);
extern "C" COMMON_API int __statfs64 (const char *file, struct statfs64 *buf);

inline int statfs (const char *file, struct statfs *buf) {
    return __statfs (file, buf);
}   

inline int statfs64 (const char *file, struct statfs64 *buf) {
    return __statfs64 (file, buf);
}   

/* access */
#define F_OK    0
#define X_OK    1
#define W_OK    2
#define R_OK    4

#ifndef D_OK
# define D_OK (R_OK + W_OK + X_OK + F_OK + 1)
#endif

#ifndef ISDIRSEP
# define ISDIRSEP(c) (c == '/')
#endif

extern "C" COMMON_API int __access (const char * file, int type);

inline int access (const char * file, int type) {
    return __access (file, type);
}

inline int file_exists(const char *filename)
{
	return (__access(filename, F_OK) != -1);
}

#ifdef WIN32
inline const std::string & win_to_unix_path(std::string & path)
{
    std::replace (path.begin(), path.end(), '\\', '/');
    return path;
}

inline const std::string & unix_to_win_path(std::string & path)
{
    std::replace (path.begin(), path.end(), '/', '\\');
    return path;
}
#endif

#endif // STATFS_H



