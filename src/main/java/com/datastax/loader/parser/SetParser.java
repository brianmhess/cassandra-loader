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

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.io.StringReader;
import java.io.IOException;
import java.text.ParseException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class SetParser extends AbstractParser {
    private Parser parser;
    private char collectionDelim;
    private char collectionBegin;
    private char collectionEnd;
    private char collectionQuote = '\"';
    private char collectionEscape = '\\';
    private String collectionNullString = "null";
    private Set<Object> elements;
    
    private CsvParser csvp = null;

    public SetParser(Parser inParser, char inCollectionDelim, 
                     char inCollectionBegin, char inCollectionEnd) {
        parser = inParser;
        collectionDelim = inCollectionDelim;
        collectionBegin = inCollectionBegin;
        collectionEnd = inCollectionEnd;
        elements = new HashSet<Object>();

        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.getFormat().setDelimiter(collectionDelim);
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
        String[] row = csvp.parseLine(toparse);
        elements.clear();
        try {
            for (int i = 0; i < row.length; i++) 
                elements.add(parser.parse(row[i]));
        }
        catch (Exception e) {
            System.err.println("Trouble parsing : " + e.getMessage());
            e.printStackTrace();
            return null;
        }
        return elements;
    }

    //public String format(Row row, int index) {
    //  if (row.isNull(index))
    //      return null;
    //  Set<Object> set = row.getSet(index, Object.class);
    @SuppressWarnings("unchecked")
    public String format(Object o) {
        Set<Object> set = (Set<Object>)o;
        Iterator<Object> iter = set.iterator();
        StringBuilder sb = new StringBuilder();
        sb.append(collectionBegin);
        if (iter.hasNext())
            sb.append(parser.format(iter.next()));
        while (iter.hasNext()) {
            sb.append(collectionDelim);
            sb.append(parser.format(iter.next()));
        }
        sb.append(collectionEnd);

        return quote(sb.toString());
    }
}
