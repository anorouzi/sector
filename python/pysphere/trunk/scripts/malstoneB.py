#!/usr/bin/env python 

import re
import datetime
import sys

def getISO8601( ts ):
    date = datetime( int( ts[0] ), int( ts[1] ), int( ts[2] ), int( ts[3] ), 0, 0, 0 )
    iso = date.isocalendar()
    #yyyyww
    return str( iso[0] ) + '%02d' % iso[1]


def parse( line ):
    return line.split('|')


def map( line, seperator='\t' ):
    print( sys.path )
    pattern = re.compile(r'\W')
    data = parse( line )
    yyyyww = getISO8601(pattern.split(data[1]))
    # <site id> \t <weekyear year><week year week>.indicator
    return "%s%s%s%s%s" % (data[2], seperator, yyyyww, ".", data[3])


def finish_key(timeslices, key):
   running_compromised = 0
   running_total = 0
   out = ''
   for (k,v) in sorted(timeslices.items()):
       running_compromised = running_compromised + int(v[0])
       running_total = running_total  + int(v[1])
       out = out + '|' +  k + ' ' + str(running_compromised) + ' ' + str(running_total)

   return '%s\t%s' % (key, out)


def reduce (data):

    last_key = None
    dict = {}

    lines = data.splitlines()
    results = []

    for line in lines:
        if( line.find( '\t' ) < 0 ):
            continue
        
        (key, value) = line.split('\t')

        # First key or a new one 
        if last_key and last_key != key:
            # collecting
            results.append(finish_key(dict, last_key))

            #  starting new record
            (last_key, result) = (key, ( value.split('.')[0], value.split('.')[1] ) )
            dict[result[0]] = (result[1].rstrip('\n'), 1)

        # This key is the same as the last key.
        # Perform the reduction operation
        else:
            (last_key, result) = (key, ( value.split('.')[0], value.split('.')[1] ) )
            old = dict.get(result[0], (0,0))
            dict[result[0]] = (int(old[0]) + int(result[1].strip('\n')), int(old[1]) + 1 )

    # Finish the last key processed. 
    if last_key:
        results.append(finish_key(dict, last_key))
        dict = {}

    return results

