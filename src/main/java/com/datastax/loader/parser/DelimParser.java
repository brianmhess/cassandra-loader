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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class DelimParser {
    private List<Parser> parsers;
    private int parsersSize;
    private List<Object> elements;
    private String delimiter;
    private String nullString;
    private char delim;
    private char quote;
    private char escape;
    private List<Boolean> skip;

    public static String DEFAULT_DELIMITER = ",";
    public static String DEFAULT_NULLSTRING = "null";

    public DelimParser() {
	this(DEFAULT_DELIMITER);
    }

    public DelimParser(String inDelimiter) {
	this(inDelimiter, DEFAULT_NULLSTRING);
    }

    public DelimParser(String inDelimiter, String inNullString) {
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
		delim = ("\\t".equals(delimiter)) ?  '\t' : delimiter.charAt(0);
		quote = '\"';
		escape = '\\';
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
	return parseComplex(line);
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
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			pe.printStackTrace(pw);
			System.err.println(sw.toString());
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
		StringBuilder retVal = new StringBuilder(parsers.get(0).format(row, 0));
		String s;
		for (int i = 1; i < parsersSize; i++) {
			s = parsers.get(i).format(row, i);
			if (null == s)
				s = nullString;
			retVal.append(delimiter).append(s);
		}
		return retVal.toString();
	}
}
