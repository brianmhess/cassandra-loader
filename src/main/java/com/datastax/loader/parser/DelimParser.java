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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

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
	if (delimiter.equals("|"))
	    delimiter = "\\|";
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
	if (trimmedToParse.startsWith("\"") && trimmedToParse.endsWith("\""))
	    trimmedToParse = trimmedToParse.substring(1, trimmedToParse.length() - 1);
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
	String[] newcolumns = new String[114];
	
	
	for (int i = 0; i < columns.length-1; i++) {
		if(i==113){
			newcolumns[113]="O";
		}else{
			newcolumns[i]=columns[i];	
		}		
	}
	if (parsersSize != newcolumns.length) {
	    System.err.println(String.format("Invalid input: Expected %d elements, found %d", parsersSize, newcolumns.length));
	    return null;
	}
	elements.clear();
	for (int i = 0; i < parsersSize; i++) {
	    try {
		String toparse = prepareToParse(newcolumns[i]);
		//MDP Change -START
		if(null==toparse){			
			if(parsers.get(i).getClass().isInstance( new com.datastax.loader.parser.DateParser(""))){
				toparse="1900-01-01 00:00:00";				
			}else if(parsers.get(i).getClass().isInstance( new com.datastax.loader.parser.StringParser())){
				toparse=" ";
			}else if(parsers.get(i).getClass().isInstance( new com.datastax.loader.parser.BigDecimalParser())){
				toparse="0";
			}else if(parsers.get(i).getClass().isInstance( new com.datastax.loader.parser.IntegerParser())){
				toparse="0";
			}			
		}
		//MDP Change -END
		elements.add(parsers.get(i).parse(toparse));
	    }
	    catch (NumberFormatException e) {
		System.err.println(String.format("Invalid number in input number %d ('%s'): %s", i, newcolumns[i], e.getMessage()));
		return null;
	    }
	    catch (ParseException pe) {
		System.err.println(String.format("Invalid format in input %d ('%s'): %s", i, newcolumns[i], pe.getMessage()));
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
	String retVal = parsers.get(0).format(row, 0);
	for (int i = 1; i < parsersSize; i++) {
	    retVal = retVal + delimiter + parsers.get(i).format(row, i);
	}
	return retVal;
    }

}
