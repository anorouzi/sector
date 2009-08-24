#
# Copyright (C) 2008-2009  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CCFLAGS = -I$(SECTOR_HOME)/common -I$(SECTOR_HOME)/client -I$(SECTOR_HOME)/gmp -I$(SECTOR_HOME)/udt -I$(SECTOR_HOME)/security -I$(JDK_HOME)/include -I$(JDK_HOME)/include/linux

LDFLAGS = -L$(SECTOR_HOME)/lib -lclient -lsecurity -lrpc -ludt -lcommon -lssl -lpthread -lstdc++

default: SectorJniClient.so

SectorJniClient.so: SectorJniClient.cpp
	g++ -fPIC $(CCFLAGS) $(LDFLAGS) -o libSectorJniClient.so -shared $^ -lstdc++

clean:
	rm -f *.so *~