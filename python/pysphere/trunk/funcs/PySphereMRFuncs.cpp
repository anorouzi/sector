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
#include <iostream>
#include <fstream>
#include <cstring>
#include <set>
#include <vector>
#include <sphere.h>
#include <Python.h>
#include <dlfcn.h>

using namespace std;

/*
  Implementation of Sphere MapReduce functions which wrap Python functions.
  Sphere is used for reading/writing data to/from Sector, and these functions
  pass the data to the Python map and reduce functions for the actual
  processing.
*/
extern "C"
{
    const int RECORD_LEN = 1024;
    const int64_t CHUNK_SIZE = 25000;

    char* pyfile = NULL;
    int partitionField;
    int numBuckets;

    /*
     1. Load the Python script and map() function.
     2. Read data from Sector.
     3. Pass the data to the Python function.
     4. Retrieve the output data from the Python function.
     5. Write the data back to Sector.
    */
    int PySphereMRFuncs_map(const SInput* input, SOutput* output, SFile* file)
    {
        // Get path to input data file to be processed:
        string filename = file->m_strHomeDir + input->m_pcUnit;
        cout << "processing " << filename << endl;

        // The input param should be a pipe delimited string containing the
        // Python script name, the field to partition on, and the number of
        // buckets to use. 
        if( pyfile == NULL ) {
            char* arg = input->m_pcParam;
            pyfile = arg;
            char* p = pyfile;
            while( *p != '|' ) ++p;
            *p = '\0';
            p++;
            partitionField = atoi( p );
            while( *p != '|' ) ++p;
            p++;
            numBuckets = atoi( p );
        }
        
        // Open input data file:
        ifstream ifs( filename.c_str() );
        if( ifs.bad() || ifs.fail() ) {
            return( 0 );
        }
    
        // Initialize SOutput struct:
        output->m_iRows = 0;
        output->m_pllIndex[0] = 0;

        
        // Variables for the embedded Python interpreter:
        PyObject *pName, *pModule, *pFunc, *pArgs, *pValue;

        // Initialize the Python interpreter and load the Python MapReduce
        // script:
        Py_Initialize();
        pName = PyString_FromString( pyfile );
        if( pName == NULL ) {
            cerr << "map() - Failed to get name of Python MapReduce script" << endl;
            return( -1 );
        }
        pModule = PyImport_Import( pName );
        Py_DECREF(pName);
        if( pModule == NULL ) {
            cerr << "map() - Failed to import Python MapReduce script" << endl;
            return( -1 );
        }

        // Get map function from Python module:
        pFunc = PyObject_GetAttrString( pModule, "map" );
        if( !( pFunc && PyCallable_Check( pFunc ) ) ) {
            if( PyErr_Occurred() ) {
                PyErr_Print();
            }
            cerr << "Failed to get map() function from Python MapReduce script" << endl;
            return( -1 );
        }

        // Number of records processed counter:
        int64_t recordsProcessed = 0;
        // Buffer for input records:
        char* buffer = new char[RECORD_LEN];

        // Set file pointer to last offset:
        ifs.seekg( output->m_llOffset );

        // Read each record in the input data file and pass it to the Python
        // map function for processing:
        while( !ifs.eof() && recordsProcessed++ < CHUNK_SIZE ) {
            
            ifs.getline( buffer, RECORD_LEN );
            if( strlen( buffer ) <= 0 ) {
                continue;
            }

            // Call the Python map function, passing in the input data:
            pArgs = PyTuple_New( 1 );
            pValue = PyString_FromString( buffer );
            PyTuple_SetItem( pArgs, 0, pValue );
            pValue = PyObject_CallObject( pFunc, pArgs );
            Py_DECREF( pArgs );
            if( pValue == NULL ) {
                Py_DECREF( pFunc );
                Py_DECREF( pModule );
                PyErr_Print();
                cerr << "Call to map() failed" << endl;
                return( -1 );
            }

            // Send the processing results back to Sector:
            sprintf( output->m_pcResult + output->m_pllIndex[output->m_iRows],
                     "%s\n",  PyString_AsString( pValue ) );
            Py_DECREF( pValue );
            int len = strlen( output->m_pcResult +
                              output->m_pllIndex[output->m_iRows] );
            output->m_iRows++;
            output->m_pllIndex[output->m_iRows] =
                output->m_pllIndex[output->m_iRows - 1] + len;
        }
   
        delete [] buffer;

        // Decrement reference counts for Python function and module objects and
        // shut down the Python interpreter:
        Py_XDECREF( pFunc );
        Py_DECREF( pModule );
        Py_Finalize();
   
        output->m_iResSize = output->m_pllIndex[output->m_iRows];

        if( ifs.eof() ) {
            output->m_llOffset = -1;
        } else {
            output->m_llOffset = ifs.tellg();
        }

        ifs.close();

        return 0;
    }

    /*
      "Bucket" the output data from the map function. This uses a simple
      calculation based of the partition field modulus the number of buckets.
    */
    int PySphereMRFuncs_partition( const char* record, int size,
                                   void* param, int psize )
    {
        if( ( NULL == record ) ||
            ( 0 == size ) ) {
            return( 0 );
        }

        char temp[size];
        strncpy( temp, record, size );

        char* fields[partitionField];
        fields[0] = temp;
        char* p = temp;
        for( int i = 1; i <= partitionField; ++i ) {
            while( ( *p != '\t' ) && ( *p != ' ' ) && ( *p != '|' ) ) {
                ++p;
            }
            *p = '\0';
            fields[i] = ++p;
        }

        return( atoi( fields[partitionField - 1] ) % numBuckets );
    }

    /*
     Order the output data from the map function.
    */
    int PySphereMRFuncs_compare( const char* record1, int size1,
                                 const char* record2, int size2 )
    {
        char temp1[size1], temp2[size2];
        strncpy( temp1, record1, size1 );
        strncpy( temp2, record2, size2 );

        char* record1Fields[partitionField];
        record1Fields[0] = temp1;
        char* p = temp1;
        for( int i = 1; i <= partitionField; ++i ) {
            while( ( *p != '\t' ) && ( *p != ' ' ) && ( *p != '|' ) ) {
                ++p;
            }
            *p = '\0';
            record1Fields[i] = ++p;
        }

        char* record2Fields[partitionField];
        record2Fields[0] = temp2;
        p = temp2;
        for( int i = 1; i <= partitionField; ++i ) {
            while( ( *p != '\t' ) && ( *p != ' ' ) && ( *p != '|' ) ) {
                ++p;
            }
            *p = '\0';
            record2Fields[i] = ++p;
        }

        int res = strcmp( record1Fields[partitionField-1],
                          record2Fields[partitionField-1] );

        return res;
    }

    /*
     1. Read data from Sector.
     2. Pass the data to the Python function.
     3. Retrieve the output data from the Python function.
     4. Write the data back to Sector.
    */
    int PySphereMRFuncs_reduce( const SInput* input, SOutput* output,
                                SFile* file )
    {
        // Variables for the embedded Python interpreter:
        PyObject *pName, *pModule, *pFunc, *pArgs, *pValue;

        // Initialize SOutput struct:
        output->m_iRows = 0;
        output->m_pllIndex[0] = 0;

        // Initialize the Python interpreter and load the Python MapReduce
        // script:
        Py_Initialize();
        pName = PyString_FromString( pyfile );
        if( pName == NULL ) {
            cerr << "reduce() - Failed to get name of Python MapReduce script" << endl;
            return( -1 );
        }
        pModule = PyImport_Import( pName );
        Py_DECREF(pName);
        if( pModule == NULL ) {
            cerr << "reduce() - Failed to import Python MapReduce script" << endl;
            return( -1 );
        }

        // Get reduce function from Python module:
        pFunc = PyObject_GetAttrString(pModule, "reduce");
        if( !( pFunc && PyCallable_Check( pFunc ) ) ) {
            if( PyErr_Occurred() ) {
                PyErr_Print();
            }
            cerr << "Failed to get reduce() function from Python MapReduce script" << endl;
            return( -1 );
        }

        // Call the Python reduce function, passing in the input data:
        pArgs = PyTuple_New( 1 );
        pValue = PyString_FromString( input->m_pcUnit );
        PyTuple_SetItem( pArgs, 0, pValue );
        pValue = PyObject_CallObject( pFunc, pArgs );
        Py_DECREF( pArgs );
        if( pValue == NULL ) {
            Py_DECREF(pFunc);
            Py_DECREF(pModule);
            PyErr_Print();
            cerr << "Call to reduce() failed" << endl;
            return( -1 );
        }

        Py_ssize_t listSize = PyList_Size( pValue );

        // Return from Python function is a list. Iterate through the list
        // and output the results to Sector:
        for( Py_ssize_t i = 0; i < listSize; i++ ) {
            PyObject* item = PyList_GET_ITEM( pValue, i );
            const char* record = PyString_AsString( item );
            sprintf( output->m_pcResult + output->m_pllIndex[output->m_iRows],
                     "%s\n", record );
            output->m_iRows++;
            output->m_pllIndex[output->m_iRows] =
                output->m_pllIndex[output->m_iRows - 1] + strlen( record ) + 1;
        }

        output->m_iResSize = output->m_pllIndex[output->m_iRows];

        // Decrement reference counts for Python function and module objects and
        // shut down the Python interpreter:
        Py_XDECREF( pFunc );
        Py_DECREF( pModule );
        Py_Finalize();
    
        return 0;
    }
}
