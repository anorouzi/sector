__copyright__ = '''
Copyright (C) 2008-2009  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Capital LLC.)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
'''

"""
Read a file from Sector and write it to the local filesystem.

Usage: python read.py src_file dest_file
"""
import sys
import sector

if( len( sys.argv ) != 3 ):
    print "Usage: python read.py <src file> <dest file>"
    sys.exit( 1 )
    
src = sys.argv[1]
dest = sys.argv[2]

sector.init( "localhost", 6000 )
sector.login( "test", "xxx", "/opt/sector/conf/master_node.cert" )
f = sector.open( src, 1 )
out = open( dest, "w" )
while True:
    s = sector.read( f, 4096 )
    if( len( s ) == 0 ):
        break
    out.write( s )
out.close()
sector.close( f )
sector.logout()
sector.closeclient()
