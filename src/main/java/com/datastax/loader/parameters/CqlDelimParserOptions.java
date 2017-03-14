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

public CqlDelimParserParameters {
    private String cqlSchema = null;
    private String table = null;
    private String keyspace = null;
    private String format = "delim";
    private Locale locale = null;
    private BooleanParser.BoolStyle boolStyle = null;
    private String dateFormatString = null;
    private String localDateFormatString = "yyyy-MM-dd";
    private String skipCols = null;

    private DelimParserParameters delimParserParameters = new DelimParserParameters();

    public CqlDelimParserParameters() { }
    
    public boolean parseArgs(Map<String,String> amap) {
        String tmap;
        if (null != (tkey = amap.remove("-format")))        format = tkey;
        if (null != (tkey = amap.remove("-schema")))        cqlSchema = tkey;
        if (null != (tkey = amap.remove("-table")))         table = tkey;
        if (null != (tkey = amap.remove("-keyspace")))      keyspace = tkey;
        if (null != (tkey = amap.remove("-dateFormat")))    dateFormatString = tkey;
        if (null != (tkey = amap.remove("-localDateFormat")))    localDateFormatString = tkey;
        if (null != (tkey = amap.remove("-skipCols")))      skipCols = tkey;

        if (null != (tkey = amap.remove("-boolStyle"))) {
            boolStyle = BooleanParser.getBoolStyle(tkey);
            if (null == boolStyle) {
                System.err.println("Bad boolean style.  Options are: " + BooleanParser.getOptions());
                return false;
            }
        }
        if (null != (tkey = amap.remove("-decimalDelim"))) {
            if (tkey.equals(","))
                locale = Locale.FRANCE;
        }

        if (null != keyspace)
            keyspace = quote(keyspace);
        if (null != table)
            table = quote(table);

        if (!delimParserParameters.parseArgs(amap))
            return false;

        return validateArgs();
    }

    public boolean validateArgs() {
        if (format.equalsIgnoreCase("delim")) {
            if (null == cqlSchema) {
                System.err.println("If you specify format " + format + " you must provide a schema");
                return false;
            }
            if (null != keyspace)
                System.err.println("Format is " + format + ", ignoring keyspace");
            if (null != table)
                System.err.println("Format is " + format + ", ignoring table");
        }
        else if (format.equalsIgnoreCase("jsonline") 
                 || format.equalsIgnoreCase("jsonarray")) {
            if (null == keyspace) {
                System.err.println("If you specify format " + format + " you must provide a keyspace");
                return false;
            }
            if (null == table) {
                System.err.println("If you specify format " + format + " you must provide a table");
                return false;
            }
            if (null != cqlSchema)
                System.err.println("Format is " + format + ", ignoring schema");
        }
        else {
            System.err.println("Unknown format option");
            return false;
        }
        
        return true;
    }

    public String usage() {
        StringBuilder usage = new StringBuilder();
        usage.append("  -format <format>                   File format: delim, jsonarray, jsonline [delim]\n");
        usage.append("  -schema <schema>                   Table schema (when using delim)\n");
        usage.append("  -table <tableName>                 Table name (when using json)\n");
        usage.append("  -keyspace <keyspaceName>           Keyspace name (when using json)\n");
        usage.append("  -dateFormat <dateFormatString>     Date format for TIMESTAMP [default for Locale.ENGLISH]\n");
        usage.append("  -localDateFormat <formatString>    Date format for DATE [yyyy-MM-dd]\n");
        usage.append("  -skipCols <columnsToSkip>          Comma-separated list of columsn to skip in the input file\n");
        usage.append("  -decimalDelim <decimalDelim>       Decimal delimiter [.] Other option is ','\n");
        usage.append("  -boolStyle <boolStyleString>       Style for booleans [TRUE_FALSE]\n");

        
        usage.append(delimParserParameters.usage());
        return usage.toString();
    }

    private String quote(String instr) {
        String ret = instr;
        if ((ret.startsWith("\"")) && (ret.endsWith("\"")))
            ret = ret.substring(1, ret.length() - 1);
        return "\"" + ret + "\"";
    }
}
