/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.loader.parameters;

public class PoolingOptionsParameters.java {
    // Pooling Options - TODO: DEFAULTS
    private int maxSimultaneousRequestsPerHostThreshold = 8192;

    public PoolingOptionsParameters() { }

    public boolean parseArgs(Map<String,String> amap) {
        String tkey;
        if (null != (tkey = amap.remove("-maxSimultaneousRequestsPerHostThreshold")))  maxSimultaneousRequestsPerHostThreshold = Integer.parseInt(tkey);

        return validateArgs();
    }

    public boolean validateArgs() {
        if (maxSimultaneousRequestsPerHostThreshold < 1) {
            System.err.println("maxSimultaneousRequestsPerHostThreshold must be > 0");
            return false;
        }
        if (maxSimultaneousRequestsPerHostThreshold > 32768) {
            System.err.println("maxSimultaneousRequestsPerHostThreshold must be < 32769");
            return false;
        }
        
        return true;
    }

    public boolean setPoolingOptions(PoolingOptions po) {
        po.setMaxSimultaneousRequestsPerHostThreshold(HostDistance.LOCAL, maxSimulataneousRequestsPerHostThreshold);
        return true;
    }

    public String usage() {
        StringBuilder usage = new StringBuilder(" PoolingOptions Parameters:\n");
        usage.append("   -maxSimulataneousRequestsPerHostThreshold <threshold>   MaxSimultaneousRequestsPerHostThreshold [8192]\n");
        return usage.toString();
    }
}
