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
package com.datastax.loader.parser;

import java.util.Map;
import java.util.Iterator;
import java.io.StringReader;
import java.text.ParseException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class UdtParser extends AbstractParser<UDTValue> {
    private Map<String,Parser> pmap = null;
    private UserType ut;
    private CsvParser csvp;

    public UdtParser(Map<String,Parser> inPmap, UserType inUt) {
        pmap = inPmap;
        ut = inUt;

        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator(",");
        settings.getFormat().setNormalizedNewline(',');
        settings.getFormat().setDelimiter(':');
        settings.getFormat().setQuote('\"');
        settings.getFormat().setQuoteEscape('\\');
        settings.getFormat().setCharToEscapeQuoteEscaping('\\');
        settings.setKeepQuotes(true);
        settings.setKeepEscapeSequences(true);

        csvp = new CsvParser(settings);
    }
    
    @SuppressWarnings("unchecked")
    public UDTValue parseIt(String toparse) throws ParseException {
        UDTValue u = ut.newValue();
        if (null == toparse)
            return null;
        toparse = unquote(toparse);
        if (!toparse.startsWith("{"))
            throw new ParseException("Must begin with {\n", 0);
        if (!toparse.endsWith("}"))
            throw new ParseException("Must end with }\n", 0);
        toparse = toparse.substring(1, toparse.length() - 1);

        StringReader sr = new StringReader(toparse);
        csvp.beginParsing(sr);
        try {
            String[] row;
            while ((row = csvp.parseNext()) != null) {
                //System.err.println("udt row[0]: " + row[0]);
                //System.err.println("udt row[1]: " + row[1]);
                String key = row[0];
                Parser p = pmap.get(key);
                //System.err.println("p: " + p);
                if (null == p) {
                    throw new ParseException("UDT element not found", 1);
                }
                Object o = p.parse(row[1]);
                Class klass = o.getClass();
                u.set(key, klass.cast(o), klass);
                //u.set(key, p.parse(row[1]), p.parse(row[1]).getClass());
            }
        }
        catch (Exception e) {
            System.err.println("Trouble parsing : " + e.getMessage());
            e.printStackTrace();
            return null;
        }        

        return u;
    }    

    @SuppressWarnings("unchecked")
    public String format(Object o) {
        UDTValue u = (UDTValue)o;
        Iterator<String> it = u.getType().getFieldNames().iterator();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        while (it.hasNext()) {
            String key = it.next();
            Object ob = u.getObject(key);
            if (null != ob) {
                Parser p = pmap.get(key);
                if (first) {
                    sb.append("{");
                    first = false;
                }
                else {
                    sb.append(",");
                }
                sb.append(key);
                sb.append(":");
                sb.append(p.format(ob));
            }
        }
        if (!first) {
            sb.append("}");
        }

        return quote(sb.toString());
    }


}
