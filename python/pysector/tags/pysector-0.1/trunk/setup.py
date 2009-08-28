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
from distutils.core import setup, Extension

sector_module = Extension( '_sector',
                           define_macros = [( 'MAJOR_VERSION', '0' ),
                                            ( 'MINOR_VERSION', '2' )],
                           include_dirs = ['/opt/sector/client',
                                           '/opt/sector/gmp',
                                           '/opt/sector/common',
                                           '/opt/sector/udt',
                                           '/opt/sector/security'],
                           libraries = ['client',
                                        'security',
                                        'rpc',
                                        'common',
                                        'udt',
                                        'ssl',
                                        'stdc++',
                                        'pthread'],
                           library_dirs = ['/opt/sector/lib'],
                           sources = ['sector.cpp', 'sector_wrap.cxx'] )

setup( name="pysector", version="0.2", ext_modules = [sector_module],
       url='http://code.google.com/p/cloud-interop/',
       author='Jonathan Seidman',
       author_email='jonathan.seidman@opendatagroup.com' )
