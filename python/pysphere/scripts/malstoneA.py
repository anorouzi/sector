#!/usr/bin/env python

def parse( line ):
    return line.split( '|' )
    
def map(line, separator='\t'):

    data = parse(line)
    return data[2] + separator + data[3]

def partition(line):

    if(line.find('\t') < 0):
        return 0
    
    # Parse out site ID:
    (key, value) = line.split('\t')

    return(int(key) % (16 * 1))

def reduce(data):

    (last_key, result) = (None, (0,0) )

    lines = data.splitlines()
    results = []

    for line in lines:
        if(line.find('\t') < 0):
            continue
        
        (key, value) = line.split('\t')

        # First key or a new one 
        if last_key and last_key != key:
            # collecting
            results.append( "%s\t%s" % ( last_key, result ) )
            #  starting new record
            (last_key, result) = (key, (int(value), 1) )

        # This key is the same as the last key.
        # Perform the reduction operation
        else:
            (last_key, result) = (key, (result[0] + int(value), result[1] + 1) )

    # Finish the last key processed. 
    if last_key:
        results.append( "%s\t%s" % ( key, result ) )

    return results
