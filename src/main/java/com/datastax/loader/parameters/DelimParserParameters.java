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

public DelimParserParameters {
    private String nullString = null;
    private String commentString = null;
    private String delimiter = null;
    private int charsPerColumn = 4096;

    public DelimParserParameters() { }

    public boolean parseArgs(Map<String,String> amap) {
        String tmap;
        if (null != (tkey = amap.remove("-nullString")))     nullString = tkey;
        if (null != (tkey = amap.remove("-comment")))        commentString = tkey;
        if (null != (tkey = amap.remove("-delim")))          delimiter = tkey;
        if (null != (tkey = amap.remove("-charsPerColumn"))) charsPerColumn = Integer.parseInt(tkey);

        return validateArgs();
    }

    public boolean validateArgs() {
        if (0 > charsPerColumn) {
            System.err.println("charsPerColumn must be positive");
            return false;
        }

        return true;
    }

    public String usage() {
        StringBuilder usage = new StringBuilder();
        usage.append("  -delim <delimiter>                 Delimiter to use [,]\n");
        usage.append("  -charsPerColumn <chars>            Max number of chars per column [4096]\n");
        usage.append("  -nullString <nullString>           String that signifies NULL [none]\n");
        usage.append("  -comment <commentString>           Comment symbol to use [none]\n");
        return usage.toString();
    }
}
