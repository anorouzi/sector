/**
 * Copyright (C) 2008-2011  Open Data ("Open Data" refers to
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

package com.opendatagroup.sector.sectorjni;

/**
 * JNI representation of Master Stat struct.
 */
public class MasterStat {
    
    /** ID of master node */
    private int id;
    
    /** IP address of master node */
    private String ip;
    
    /** Port number of master node */
    private int port;

    /**
     * Get the id.
     * @return int
     */
    public int getId() {
        return id;
    }

    /**
     * Set the id.
     * @param newId int
     */
    public void setId(int newId) {
        id = newId;
    }

    /**
     * Get the ip.
     * @return String
     */
    public String getIp() {
        return ip;
    }

    /**
     * Set the ip.
     * @param newIp String
     */
    public void setIp(String newIp) {
        ip = newIp;
    }

    /**
     * Get the port.
     * @return int
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the port.
     * @param newPort int
     */
    public void setPort(int newPort) {
        port = newPort;
    }

}
