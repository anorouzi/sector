#!/bin/sh

libtoolize &&
aclocal &&
automake --add-missing --copy &&
${AUTOCONF:-autoconf}

