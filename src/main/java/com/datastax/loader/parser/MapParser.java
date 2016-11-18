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
import java.util.Set;
import java.util.HashMap;
import java.util.Iterator;
import java.io.StringReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class MapParser extends AbstractParser {
    private Parser keyParser;
    private Parser valueParser;
    private char collectionDelim;
    private char collectionBegin;
    private char collectionEnd;
    private char collectionQuote = '\"';
    private char collectionEscape = '\\';
    private char mapDelim;
    private String collectionNullString = null;
    private Map<Object,Object> elements;
    
    private CsvParser csvp = null;

    public MapParser(Parser inKeyParser, Parser inValueParser,
                     char inCollectionDelim, char inCollectionBegin, 
                     char inCollectionEnd, char inMapDelim) {
        keyParser = inKeyParser;
        valueParser = inValueParser;
        collectionDelim = inCollectionDelim;
        collectionBegin = inCollectionBegin;
        collectionEnd = inCollectionEnd;
        mapDelim = inMapDelim;
        elements = new HashMap<Object,Object>();

        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("" + collectionDelim);
        settings.getFormat().setNormalizedNewline(collectionDelim);
        settings.getFormat().setDelimiter(mapDelim);
        settings.getFormat().setQuote(collectionQuote);
        settings.getFormat().setQuoteEscape(collectionEscape);
        settings.getFormat().setCharToEscapeQuoteEscaping(collectionEscape);
        settings.setKeepQuotes(true);
        settings.setKeepEscapeSequences(true);
        
        csvp = new CsvParser(settings);
    }
    public Object parse(String toparse) throws ParseException {
        if (null == toparse)
            return null;
        toparse = unquote(toparse);
        if (!toparse.startsWith(Character.toString(collectionBegin)))
            throw new ParseException("Must begin with " + collectionBegin 
                                     + "\n", 0);
        if (!toparse.endsWith(Character.toString(collectionEnd)))
            throw new ParseException("Must end with " + collectionEnd 
                                     + "\n", 0);
        toparse = toparse.substring(1, toparse.length() - 1);
        elements.clear();
        StringReader sr = new StringReader(toparse);
        csvp.beginParsing(sr);
        try {
            String[] row;
            while ((row = csvp.parseNext()) != null) {
                Object key = keyParser.parse(row[0]);
                Object value = valueParser.parse(row[1]);
                elements.put(key, value);
            }
        }
        catch (Exception e) {
            System.err.println("Trouble parsing : " + e.getMessage());
            e.printStackTrace();
            return null;
        }
        return elements;
    }

    @SuppressWarnings("unchecked")
    public String format(Object o) {
        Map<Object,Object> map = (Map<Object,Object>)o;
        Iterator<Map.Entry<Object,Object> > iter = map.entrySet().iterator();
        Map.Entry<Object,Object> me;
        StringBuilder sb = new StringBuilder();
        sb.append(collectionBegin);
        if (iter.hasNext()) {
            me = iter.next();
            sb.append(keyParser.format(me.getKey()));
            sb.append(mapDelim);
            sb.append(valueParser.format(me.getValue()));
        }
        while (iter.hasNext()) {
            sb.append(collectionDelim);
            me = iter.next();
            sb.append(keyParser.format(me.getKey()));
            sb.append(mapDelim);
            sb.append(valueParser.format(me.getValue()));
        }
        sb.append(collectionEnd);

        return quote(sb.toString());
    }
}
