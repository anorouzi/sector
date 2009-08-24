/*
 * Copyright (C) 2008-2009  Open Data ("Open Data" refers to
 * one or more of the following companies: Open Data Partners LLC,
 * Open Data Research LLC, or Open Data Capital LLC.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
#include <dcclient.h>
#include <util.h>
#include <iostream>
#include <getopt.h>

using namespace std;

int getfilenames( vector<string>&, char* );

/*
  A framework for executing Python MapReduce functions from inside Sphere.

  Usage: usage: PySphereMR <-c config-file> <-s python-script> \
                <-d data-file | -f input-filename> [-p partition-field]

  config-file: Fully qualified path to the Sector client configuration file.
  python-script: The name of the Python script containing the map and reduce
                 functions.
  data-file: Name of a single input file to be processed.
  input-filename: Name of a text file containing the names of input files to be
                  processed, one name per line.
  partition-field: Position of field in input data to use for partitioning. This
                   argument is optional.

  Use either data-file or input-filename argument, not both.
*/
int main(int argc, char** argv)
{
    int c;
    char* configFile = NULL;
    char* pythonScript = NULL;
    char* dataFile = NULL;
    char* inputFile = NULL;
    char* field = "1";

    // Parse input arguments:
    while( ( c = getopt( argc, argv, "c:s:f:d:p:" ) ) != -1 ) {
        switch( c ) {
          case 'c':
              configFile = optarg;
              break;
          case 's':
              pythonScript = optarg;
              break;
          case 'f':
              inputFile = optarg;
              break;
          case 'd':
              dataFile = optarg;
              break;
          case 'p':
              field = optarg;
              break;
        }
    }

    if( configFile == NULL || pythonScript == NULL ||
        ( inputFile == NULL && dataFile == NULL ) ) {
        cout << "usage: PySphereMR <-c config-file> <-s python-script> <-d file | -f input-filename> [-p partition-field]" << endl;
        return( 0 );
    }

    // Initialize and log into Sector:
    Session s;
    s.loadInfo( configFile );

    if( Sector::init(
            s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0 ) {
        return( -1 );
    }
   
    if( Sector::login(
            s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword,
            s.m_ClientConf.m_strCertificate.c_str()) < 0 ) {
        return( -1 );
    }

    // Get names of input files to be processed:
    vector<string> files;
    if( inputFile != NULL ) {
        if( 0 > getfilenames( files, inputFile ) ) {
            cout << "failed to get filenames - exiting..." << endl;
            return -1;
        }
    } else {
        files.insert( files.end(), dataFile );
    }

    // Show files to be processed:
    cout << "processing files:" << endl;
    for( vector<string>::iterator iter = files.begin();
         iter != files.end();
         iter++ ) {
       
        string filename = *iter;
        cout << filename << endl;
    }

    // Init input and output streams:
    SphereStream input;
    if( input.init( files ) < 0 ) {
        cout << "unable to locate input data files. quit.\n";
        return( -1 );
    }

    // Very simple calculation for number of output buckets - just base on
    // number of slaves:
    SysStat sys;
    Sector::sysinfo( sys );
    const int32_t totalSlaves = sys.m_llTotalSlaves;
    const int32_t numBuckets = totalSlaves * 16;
    
    SphereStream output;
    output.setOutputPath( "/pysphere_mr_output", "pybucket");
    output.init( numBuckets );

    SphereProcess myproc;

    // Load the shared object containing the Sphere MR implementations:
    if( myproc.loadOperator( "./funcs/PySphereMRFuncs.so" ) < 0 ) {
        return( -1 );
    }

    timeval t;
    gettimeofday( &t, 0 );
    cout << "start time " << t.tv_sec << endl;

    // Sphere allows passing a char argument to the processing functions. We
    // need to pass the name of the Python script and the value for the field
    // to partition on, so create a single argument from the two values:
    char arg[128];
    sprintf( arg, "%s|%s|%d", pythonScript, field, numBuckets );

    // Kick off the processing:
    if( myproc.run_mr( input, output, "PySphereMRFuncs", 0,
                       arg, strlen( arg ) + 1 ) < 0) {
        cout << "failed to find any computing resources." << endl;
        return -1;
    }

    timeval t1, t2;
    gettimeofday(&t1, 0);
    t2 = t1;
    while( true ) {
        SphereResult* res;

        if( myproc.read( res ) < 0 ) {
            if( myproc.checkMapProgress() < 0 ) {
                cerr << "all SPEs failed\n";
                break;
            }

            if( myproc.checkMapProgress() == 100 ) {
                break;
            }
        }

        gettimeofday(&t2, 0);
        if( t2.tv_sec - t1.tv_sec > 60 ) {
            cout << "MAP PROGRESS: " << myproc.checkProgress() << "%" << endl;
            t1 = t2;
        }
    }

    while( myproc.checkReduceProgress() < 100 ) {
        usleep(10);
    }

    gettimeofday(&t, 0);
    cout << "mapreduce sort accomplished " << t.tv_sec << endl;

    cout << "SPE COMPLETED " << endl;
   
    myproc.close();

    Sector::logout();
    Sector::close();

    return 0;
}

int getfilenames( vector<string>& filenames, char* configfile )
{
    ifstream in( configfile );
    if( in.bad() || in.fail() ) {
        return -1;
    }

    string s;
    while( getline( in, s )  ) {
        filenames.insert(filenames.end(), s);
    }

    return( 0 );
}
