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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class DelimParser {
    private List<Parser> parsers;
    private int parsersSize;
    private List<Object> elements;
    private String delimiter;
    private int charsPerColumn;
    private String nullString;
    private char delim;
    private char quote;
    private char escape;
    private char comment;
    private List<Boolean> skip;

    private CsvParser csvp = null;

    public static String DEFAULT_DELIMITER = ",";
    public static String DEFAULT_NULLSTRING = "";
    public static String DEFAULT_COMMENT_STRING = "\0";
    public static int DEFAULT_CHARSPERCOLUMN = 4096;

    public DelimParser() {
        this(DEFAULT_DELIMITER);
    }

    public DelimParser(String inDelimiter) {
        this(inDelimiter, DEFAULT_CHARSPERCOLUMN, DEFAULT_NULLSTRING, DEFAULT_COMMENT_STRING);
    }

    public DelimParser(String inDelimiter, int inCharsPerColumn,
                       String inNullString, String inComment) {
        parsers = new ArrayList<Parser>();
        elements = new ArrayList<Object>();
        skip = new ArrayList<Boolean>();
        parsersSize = parsers.size();
        if (null == inDelimiter)
            delimiter = DEFAULT_DELIMITER;
        else
            delimiter = inDelimiter;
        if (null == inNullString)
            nullString = DEFAULT_NULLSTRING;
        else
            nullString = inNullString;
        if (null == inComment)
            comment = DEFAULT_COMMENT_STRING.charAt(0);
        else
            comment = inComment.charAt(0);
        charsPerColumn = inCharsPerColumn;
        delim = ("\\t".equals(delimiter)) ?  '\t' : delimiter.charAt(0);
        quote = '\"';
        escape = '\\';

        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        settings.getFormat().setDelimiter(delim);
        settings.setMaxCharsPerColumn(charsPerColumn);
        settings.getFormat().setQuote(quote);
        settings.getFormat().setQuoteEscape(escape);
        settings.getFormat().setComment('\0');

        csvp = new CsvParser(settings);
    }

    // Adds a parser to the list
    public void add(Parser p) {
        parsers.add(p);
        skip.add(false);
        parsersSize = parsers.size();
    }

    public void addSkip(int idx) {
        parsers.add(idx, new StringParser());
        skip.add(idx, true);
        parsersSize = parsers.size();
    }

    // This is where we apply rules like quoting, NULL, etc
    private String prepareToParse(String toparse) {
        String trimmedToParse = toparse.trim();
        if (trimmedToParse.startsWith("\"") && trimmedToParse.endsWith("\""))
            trimmedToParse = trimmedToParse.substring(1, trimmedToParse.length() - 1);
        if (trimmedToParse.equals(nullString))
            return null;
        return trimmedToParse;
    }

    public List<Object> parse(String line) {
        //return parseComplex(line);
        return parseWithUnivocity(line);
    }

    public List<Object> parseWithUnivocity(String line) {
        String[] row = csvp.parseLine(line);
        return parse(row);
    }

    public List<Object> parse(String[] row) {
        if (row.length != parsersSize) {
            System.err.println("Row has different number of fields (" + row.length + ") than expected (" + parsersSize + ")");
            return null;
        }
        elements.clear();
        Object toAdd;
        for (int i = 0; i < parsersSize; i++) {
            try {
                if ((null == row[i]) ||
                    ((null != nullString) &&
                     (nullString.equalsIgnoreCase(row[i]))))
                    toAdd = null;
                else
                    toAdd = parsers.get(i).parse(row[i]);

                if (!skip.get(i))
                    elements.add(toAdd);
            }
            catch (NumberFormatException e) {
                System.err.println(String.format("Invalid number in input number %d: %s", i, e.getMessage()));
                return null;
            }
            catch (ParseException pe) {
                System.err.println(String.format("Invalid format in input %d: %s", i, pe.getMessage()));
                return null;
            }
        }

        return elements;
    }

    public List<Object> parseComplex(String line) {
        elements.clear();
        IndexedLine sr = new IndexedLine(line);
        for (int i = 0; i < parsersSize; i++) {
            try {
                Object toAdd = parsers.get(i).parse(sr, nullString, delim, 
                                                    escape, quote, 
                                                    (parsersSize-1 == i));
                if (!skip.get(i))
                    elements.add(toAdd);
            }
            catch (NumberFormatException e) {
                System.err.println(String.format("Invalid number in input number %d: %s", i, e.getMessage()));
                return null;
            }
            catch (ParseException pe) {
                System.err.println(String.format("Invalid format in input %d: %s", i, pe.getMessage()));
                return null;
            }
            catch (IOException e) {
                System.err.println(String.format("Invalid number of fields - ran out of string: %s", i, e.getMessage()));
                return null;
            }
        }
        return elements;
    }

    // returns an array of Objects - to be used in PreparedStatement.bind()
    public Object[] getElements() {
        return elements.toArray();
    }

    public String format(Row row) throws IndexOutOfBoundsException, InvalidTypeException {
        StringBuilder retVal = new StringBuilder();
        String[] stringVals = stringVals(row);
        retVal.append(stringVals[0]);
        for (int i = 1; i < parsersSize; i++) {
            retVal.append(delimiter).append(stringVals[i]);
        }
        return retVal.toString();
    }

    public String[] stringVals(Row row) throws IndexOutOfBoundsException, InvalidTypeException {
        String[] stringVals = new String[parsers.size()];
        for (int i = 0; i < parsersSize; i++) {
            stringVals[i] = parsers.get(i).format(row, i);
            if (null == stringVals[i])
                stringVals[i] = nullString;
        }
        return stringVals;
    }
}
