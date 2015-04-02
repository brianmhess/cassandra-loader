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

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.lang.String;
import java.lang.System;
import java.lang.NumberFormatException;
import java.text.ParseException;

public class DelimParser {
    private List<Parser> parsers;
    private int parsersSize;
    private List<Object> elements;
    private String delimiter;
    private String nullString;
    private Pattern pattern;
    private static String regexString = "(?=([^\"]*\"[^\"]*\")*[^\"]*$)";


    public static String DEFAULT_DELIMITER = ",";
    public static String DEFAULT_NULLSTRING = "";

    public DelimParser() {
	this(DEFAULT_DELIMITER);
    }

    public DelimParser(String inDelimiter) {
	this(inDelimiter, DEFAULT_NULLSTRING);
    }

    public DelimParser(String inDelimiter, String inNullString) {
	this(inDelimiter, inNullString, false);
    }

    public DelimParser(String inDelimiter, String inNullString, boolean delimiterInQuotes) {
	parsers = new ArrayList<Parser>();
	elements = new ArrayList<Object>();
	parsersSize = parsers.size();
	if (null == inDelimiter)
	    delimiter = DEFAULT_DELIMITER;
	else 
	    delimiter = inDelimiter;
	if (null == inNullString)
	    nullString = DEFAULT_NULLSTRING;
	else
	    nullString = inNullString;
	if (delimiterInQuotes)
	    pattern = Pattern.compile(delimiter + regexString);
	else
	    pattern = Pattern.compile(delimiter);
    }
    
    // Adds a parser to the list
    public void add(Parser p) {
	parsers.add(p);
	parsersSize = parsers.size();
    }

    // Adds a collection of parsers to the list
    public void add(Collection<Parser> pl) {
	parsers.addAll(pl);
	parsersSize = parsers.size();
    }

    // This is where we apply rules like quoting, NULL, etc
    private String prepareToParse(String toparse) {
	String trimmedToParse = toparse.trim();
	if (trimmedToParse.equals(nullString))
	    return null;
	return trimmedToParse;
    }

    // Parses this line by applying the parsers
    // Result is a boolean of success or failure
    // Pick up the results with a call to getElements
    // returns an array of Objects - to be used in PreparedStatement.bind()
    public List<Object> parse(String line) {
	String[] columns = pattern.split(line, -1);
	if (parsersSize != columns.length) {
	    System.err.println(String.format("Invalid input: Expected %d elements, found %d", parsersSize, columns.length));
	    return null;
	}
	elements.clear();
	for (int i = 0; i < parsersSize; i++) {
	    try {
		String toparse = prepareToParse(columns[i]);
		elements.add(parsers.get(i).parse(toparse));
	    }
	    catch (NumberFormatException e) {
		System.err.println(String.format("Invalid number in input number %d ('%s'): %s", i, columns[i], e.getMessage()));
		return null;
	    }
	    catch (ParseException pe) {
		System.err.println(String.format("Invalid format in input %d ('%s'): %s", i, columns[i], pe.getMessage()));
		return null;
	    }
	}
	return elements;
    }

    // returns an array of Objects - to be used in PreparedStatement.bind()
    public Object[] getElements() {
	return elements.toArray();
    }
}
