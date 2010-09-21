// Adapted from:

/* Return the canonical absolute name of a given file.
   Copyright (C) 1996, 1997, 1998, 1999, 2000 Free Software Foundation, Inc.
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

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/stat.h>
#include <errno.h>
#include <stddef.h>

# include <stdio.h>
# include <windows.h>
# include <malloc.h> 
#define set_werrno
#include "statfs.h"

/* Return the canonical absolute name of file NAME.  A canonical name
   does not contain any `.', `..' components nor any repeated path
   separators ('/') or symlinks.  All path components must exist.  If
   RESOLVED is null, the result is malloc'd; otherwise, if the
   canonical name is PATH_MAX chars or more, returns null with `errno'
   set to ENAMETOOLONG; if the name fits in fewer than PATH_MAX chars,
   returns the name in RESOLVED.  If the name cannot be resolved and
   RESOLVED is non-NULL, it contains the path of the first component
   that cannot be resolved.  If the path can be resolved, RESOLVED
   holds the same value as the value returned.
   RESOLVED must be at least PATH_MAX long */

inline char * win2unixpath(char *FileName)
{
	char *s = FileName;
	while (*s) {
		if (*s == '\\')
			*s = '/';
		*s++;
	}
	return FileName;
}

inline char *unix2winpath(char *FileName)
{
	char *s = FileName;
	while (*s) {
		if (*s == '/')
			*s = '\\';
		*s++;
	}
	return FileName;
}

static char *
canonicalize (const char *name, char *resolved)
{
  char *rpath, *extra_buf = NULL;
  int num_links = 0, old_errno;

  if (name == NULL)
    {
      /* As per Single Unix Specification V2 we must return an error if
	 either parameter is a null pointer.  We extend this to allow
	 the RESOLVED parameter to be NULL in case the we are expected to
	 allocate the room for the return value.  */
      _set_errno (EINVAL);
      return NULL;
    }

  if (name[0] == '\0')
    {
      /* As per Single Unix Specification V2 we must return an error if
	 the name argument points to an empty string.  */
      _set_errno (ENOENT);
      return NULL;
    }


	char *lpFilePart;
	int len;
//  fprintf(stderr, "name: %s\n", name);
	rpath = resolved ? (char *)alloca (MAX_PATH) : (char *)malloc (MAX_PATH);
//	unix2winpath (name);
//  fprintf(stderr, "name: %s\n", name);
	len = GetFullPathName(name, MAX_PATH, rpath, &lpFilePart);
//  fprintf(stderr, "rpath: %s\n", rpath);
	if (len == 0) {
		set_werrno;
		return NULL;
	}
	if (len > MAX_PATH)	{
		if (resolved)
			_set_errno(ENAMETOOLONG);
		else {
			rpath = (char *)realloc(rpath, len + 2);
			GetFullPathName(name, len, rpath, &lpFilePart);
//  fprintf(stderr, "rpath: %s\n", rpath);
		}
	}
//	if ( ISDIRSEP(name[strlen(name)]) && !ISDIRSEP(rpath[len]) ) {
//		rpath[len] = '\\';
//		rpath[len + 1] = 0;
//	}
	old_errno = errno;
	if (!access (rpath, D_OK) && !ISDIRSEP(rpath[len - 1]) ){
		rpath[len] = '\\';
		rpath[len + 1] = 0;
	}
	errno = old_errno;
	win2unixpath (rpath);
//  fprintf(stderr, "rpath: %s\n", rpath);
	return resolved ? strcpy(resolved, rpath) : rpath ;

}


char *
__realpath (const char *name, char *resolved)
{
  if (resolved == NULL)
    {
      _set_errno (EINVAL);
      return NULL;
    }

  return canonicalize (name, resolved);
}
//weak_alias (__realpath, realpath)


char *
__canonicalize_file_name (const char *name)
{
  return canonicalize (name, NULL);
}

//weak_alias (__canonicalize_file_name, canonicalize_file_name)