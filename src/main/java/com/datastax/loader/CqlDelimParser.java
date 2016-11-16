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
package com.datastax.loader;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.loader.parser.BigDecimalParser;
import com.datastax.loader.parser.BigIntegerParser;
import com.datastax.loader.parser.BooleanParser;
import com.datastax.loader.parser.ByteBufferParser;
import com.datastax.loader.parser.DateParser;
import com.datastax.loader.parser.DelimParser;
import com.datastax.loader.parser.DoubleParser;
import com.datastax.loader.parser.FloatParser;
import com.datastax.loader.parser.InetAddressParser;
import com.datastax.loader.parser.IntegerParser;
import com.datastax.loader.parser.ListParser;
import com.datastax.loader.parser.LongParser;
import com.datastax.loader.parser.MapParser;
import com.datastax.loader.parser.Parser;
import com.datastax.loader.parser.SetParser;
import com.datastax.loader.parser.StringParser;
import com.datastax.loader.parser.UUIDParser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class CqlDelimParser {
    private Map<DataType.Name, Parser> pmap;
    private List<SchemaBits> sbl;
    private List<String> columnNames;
    private String keyspace;
    private String tablename;
    private DelimParser delimParser;
    private JSONParser jsonParser;

    public CqlDelimParser(String inCqlSchema, String inDelimiter, int inCharsPerColumn,
                          String inNullString, String inCommentString, String inDateFormatString,
                          BooleanParser.BoolStyle inBoolStyle, Locale inLocale,
                          String skipList, Session session, boolean bLoader) 
        throws ParseException {
        // Optionally provide things for the line parser - date format, boolean format, locale
        initPmap(inDateFormatString, inBoolStyle, inLocale, bLoader);
        processCqlSchema(inCqlSchema, session);
        createDelimParser(inDelimiter, inCharsPerColumn, inNullString, inCommentString, skipList);
    }   

    public CqlDelimParser(String inKeyspace, String inTable, String inDelimiter,
                          int inCharsPerColumn,
                          String inNullString, String inCommentString, String inDateFormatString,
                          BooleanParser.BoolStyle inBoolStyle, Locale inLocale,
                          String skipList, Session session, boolean bLoader) 
        throws ParseException {
        // Optionally provide things for the line parser - date format, boolean format, locale
        keyspace = inKeyspace;
        tablename = inTable;
        initPmap(inDateFormatString, inBoolStyle, inLocale, bLoader);
        processCqlSchema(session);
        createDelimParser(inDelimiter, inCharsPerColumn, inNullString, inCommentString,  skipList);
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    private void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    // used internally to store schema information
    private class SchemaBits {
        public String name;
        public DataType.Name datatype;
        public Parser parser;
    }

    // intialize the Parsers and the parser map
    private void initPmap(String dateFormatString, BooleanParser.BoolStyle inBoolStyle, 
                          Locale inLocale, boolean bLoader) {
        pmap = new HashMap<DataType.Name, Parser>();
        Parser integerParser = new IntegerParser(inLocale, bLoader);
        Parser longParser = new LongParser(inLocale, bLoader);
        Parser floatParser = new FloatParser(inLocale, bLoader);
        Parser doubleParser = new DoubleParser(inLocale, bLoader);
        Parser stringParser = new StringParser();
        Parser booleanParser = new BooleanParser(inBoolStyle);
        Parser uuidParser = new UUIDParser();
        Parser bigDecimalParser = new BigDecimalParser();
        Parser bigIntegerParser = new BigIntegerParser();
        Parser byteBufferParser = new ByteBufferParser();
        Parser inetAddressParser = new InetAddressParser();
        Parser dateParser = new DateParser(dateFormatString);

        pmap.put(DataType.Name.ASCII, stringParser);
        pmap.put(DataType.Name.BIGINT, longParser);
        pmap.put(DataType.Name.BLOB, byteBufferParser);
        pmap.put(DataType.Name.BOOLEAN, booleanParser);
        pmap.put(DataType.Name.COUNTER, longParser);
        pmap.put(DataType.Name.DECIMAL, bigDecimalParser);
        pmap.put(DataType.Name.DOUBLE, doubleParser);
        pmap.put(DataType.Name.FLOAT, floatParser);
        pmap.put(DataType.Name.INET, inetAddressParser);
        pmap.put(DataType.Name.INT, integerParser);
        pmap.put(DataType.Name.TEXT, stringParser);
        pmap.put(DataType.Name.TIMESTAMP, dateParser);
        pmap.put(DataType.Name.TIMEUUID, uuidParser);
        pmap.put(DataType.Name.UUID, uuidParser);
        pmap.put(DataType.Name.VARCHAR, stringParser);
        pmap.put(DataType.Name.VARINT, bigIntegerParser);
    }

    // Validate the CQL schema, extract the keyspace and tablename, and process the rest of the schema
    private void processCqlSchema(String cqlSchema, Session session) throws ParseException {
        String kstnRegex = "^\\s*(\\\"?[A-Za-z0-9_]+\\\"?)\\.(\\\"?[A-Za-z0-9_]+\\\"?)\\s*[\\(]\\s*(\\\"?[A-Za-z0-9_]+\\\"?\\s*(,\\s*\\\"?[A-Za-z0-9_]+\\\"?\\s*)*)[\\)]\\s*$";
        Pattern p = Pattern.compile(kstnRegex);
        Matcher m = p.matcher(cqlSchema);
        if (!m.find()) {
            throw new ParseException("Badly formatted schema  " + cqlSchema, 0);
        }
        keyspace = m.group(1);
        tablename = m.group(2);
        String schemaString = m.group(3);
        sbl = schemaBits(schemaString, session);
    }

    private void processCqlSchema(Session session) throws ParseException {
        sbl = schemaBits(null, session);
    }


    private List<SchemaBits> schemaBits(String in, Session session) throws ParseException {
        KeyspaceMetadata km = session.getCluster().getMetadata().getKeyspace(keyspace);
        if (null == km) {
            System.err.println("Keyspace " + keyspace + " not found.");
            System.exit(-1);
        }
        TableMetadata tm = km.getTable(tablename);
        if (null == tm) {
            System.err.println("Table " + tablename + " not found.");
            System.exit(-1);
        }
        List<String> inList = new ArrayList<String>();
        if (null != in) {
            String[] tlist = in.split(",");
            for (int i = 0; i < tlist.length; i++)
                inList.add(tlist[i].trim());
        }
        else {
            for (ColumnMetadata cm : tm.getColumns())
                inList.add("\""+cm.getName()+"\"");
        }
        //keep the list of columns from metadata to use as column backbone for JSON
        setColumnNames(inList);
        List<SchemaBits> sbl = new ArrayList<SchemaBits>();
        for (int i = 0; i < inList.size(); i++) {
            String col = inList.get(i);
            SchemaBits sb = new SchemaBits();
            ColumnMetadata cm = tm.getColumn(col);
            if (null == cm) {
                System.err.println("Column " + col + " of table " + keyspace + "." + tablename + " not found");
                System.exit(-1);
            }
            DataType dt = cm.getType();
            sb.name = col;
            sb.datatype = dt.getName();
            if (dt.isCollection()) {
                if (sb.datatype == DataType.Name.LIST) {
                    DataType.Name listType = dt.getTypeArguments().get(0).getName();
                    Parser listParser = pmap.get(listType);
                    if (null == listParser) {
                        throw new ParseException("List data type not recognized (" 
                                                 + listType + ")", i);
                    }
                    sb.parser = new ListParser(listParser, ',', '[', ']');
                }
                else if (sb.datatype == DataType.Name.SET) {
                    DataType.Name setType = dt.getTypeArguments().get(0).getName();
                    Parser setParser = pmap.get(setType);
                    if (null == setParser) {
                        throw new ParseException("Set data type not recognized (" 
                                                 + setType + ")", i);
                    }
                    sb.parser = new SetParser(setParser, ',', '{', '}');
                }
                else if (sb.datatype == DataType.Name.MAP) {
                    DataType.Name keyType = dt.getTypeArguments().get(0).getName();
                    Parser keyParser = pmap.get(keyType);
                    if (null == keyParser) {
                        throw new ParseException("Map key data type not recognized (" 
                                                 + keyType + ")", i);
                    }
                    DataType.Name valueType = dt.getTypeArguments().get(1).getName();
                    Parser valueParser = pmap.get(valueType);
                    if (null == valueParser) {
                        throw new ParseException("Map value data type not recognized (" 
                                                 + valueType + ")", i);
                    }
                    sb.parser = new MapParser(keyParser, valueParser, ',', '{', '}', ':');
                }
                else {
                    throw new ParseException("Collection data type not recognized (" 
                                             + sb.datatype + ")", i);
                }
            }
            else {
                sb.parser = pmap.get(sb.datatype);
                if (null == sb.parser) {
                    throw new ParseException("Column data type not recognized (" + sb.datatype + ")", i);
                }
            }
            sbl.add(sb);
        }
        return sbl;
    }

    // Creates the DelimParser that will parse the line
    private void createDelimParser(String delimiter, int charsPerColumn,
                                   String nullString, String commentString,
                                   String skipList) throws NumberFormatException {
        delimParser = new DelimParser(delimiter, charsPerColumn, nullString, commentString);
        for (int i = 0; i < sbl.size(); i++)
            delimParser.add(sbl.get(i).parser);
        if (null != skipList) {
            for (String s : skipList.split(",")) {
                delimParser.addSkip(Integer.parseInt(s.trim()));
            }
        }
        jsonParser = new JSONParser();
    }

    // Convenience method to return the INSERT statement for a PreparedStatement.
    public String generateInsert() {
        String insert = "INSERT INTO " + keyspace + "." + tablename + "(" + sbl.get(0).name;
        String qmarks = "?";
        for (int i = 1; i < sbl.size(); i++) {
            insert = insert + ", " + sbl.get(i).name;
            qmarks = qmarks + ", ?";
        }
        insert = insert + ") VALUES (" + qmarks + ")";
        return insert;
    }

    public String generateSelect() {
        String select = "SELECT " + sbl.get(0).name;
        for (int i = 1; i < sbl.size(); i++) {
            select = select + ", " + sbl.get(i).name;
        }
        select += " FROM " + keyspace + "." + tablename;
        return select;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return tablename;
    }

    // Pass through to parse the line - the DelimParser we created will be used.
    public List<Object> parse(String line) {
        return delimParser.parse(line);
    }

    public List<Object> parse(String[] row) {
        return delimParser.parse(row);
    }

    @SuppressWarnings("unchecked")
    public List<Object> parseJson(String line) {
        JSONObject jsonObject = null;
        try {
            jsonObject = (JSONObject)jsonParser.parse(line);
        } catch (org.json.simple.parser.ParseException e) {
            System.err.println(String.format("Invalid format in input %d: %s",line, e.getMessage()));
            return null;
        }
        String[] row = new String[columnNames.size()];
        Set<String> fields = (Set<String>)jsonObject.keySet();
        for (int i = 0; i < columnNames.size(); i++) {
            String s = columnNames.get(i);
            Object o = jsonObject.get(s);
            if (null != o)
                row[i] = o.toString();
            else row[i] = null;
            fields.remove(s);
        }
        if (0 != fields.size()) {
            for (String f : fields) {
                System.err.println("Unknown JSON field " + f);
            }
            return null;
        }
        return parse(row);
    }

    public String format(Row row) throws IndexOutOfBoundsException, InvalidTypeException {
        return delimParser.format(row);
    }

    public String formatJson(Row row) throws IndexOutOfBoundsException, InvalidTypeException {
        String[] stringVals = delimParser.stringVals(row);
        Map<String,String> pairs = new HashMap<String,String>();
        for (int i = 0; i < sbl.size(); i++)
            pairs.put(sbl.get(i).name, stringVals[i]);
        return JSONObject.toJSONString(pairs);
    }

}

