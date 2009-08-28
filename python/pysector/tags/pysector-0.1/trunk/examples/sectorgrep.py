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
Grep for a pattern inside a Sector file

Usage python sectorgrep.py  search_term file
"""

import re
import sys
import sector

if( len( sys.argv ) != 3 ):
    print "Usage: python sectorgrep.py search_term file"
    sys.exit( 1 )

searchterm = sys.argv[1]
path = sys.argv[2]

sector.init( "localhost", 6000 )
sector.login( "test", "xxx", "/opt/sector/conf/master_node.cert" )
f = sector.open( path, 1 )

p = re.compile( searchterm )

while True:
    data = sector.read( f, 4096 )
    if( len( data ) == 0 ):
        break
    lines = data.split( '\n' )
    for line in lines:
        match = p.search( line )
        if( match ):
            print line
            
sector.close( f )
sector.logout()
sector.closeclient()
