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

import java.lang.String;
import java.lang.Character;
import java.lang.StringBuilder;
import java.lang.IndexOutOfBoundsException;
import java.util.List;
import java.util.ArrayList;
import java.io.StringReader;
import java.io.IOException;
import java.text.ParseException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

// String parser - simple
public class ListParser extends AbstractParser {
    private Parser parser;
    private char collectionDelim;
    private char collectionBegin;
    private char collectionEnd;
    private char collectionQuote = '\"';
    private char collectionEscape = '\\';
    private String collectionNullString = "null";
    private List<Object> elements;
    public ListParser(Parser inParser, char inCollectionDelim, 
		      char inCollectionBegin, char inCollectionEnd) {
	collectionDelim = inCollectionDelim;
	collectionBegin = inCollectionBegin;
	collectionEnd = inCollectionEnd;
	elements = new ArrayList<Object>();
    }
    public Object parse(String toparse) throws ParseException {
	if (!toparse.startsWith(Character.toString(collectionBegin)))
	    throw new ParseException("Must begin with " + collectionBegin 
				     + "\n", 0);
	if (!toparse.endsWith(Character.toString(collectionEnd)))
	    throw new ParseException("Must end with " + collectionEnd 
				     + "\n", 0);
	toparse = toparse.substring(1, toparse.length() - 1);
	IndexedLine sr = new IndexedLine(toparse);
	String parseit;
	try {
	    while (null != (parseit = getQuotedOrUnquoted(sr, 
							  collectionNullString,
							  collectionDelim,
							  collectionEscape,
							  collectionQuote))) {
		elements.add(parser.parse(parseit));
	    }
	}
	catch (IOException ioe) {
	    System.err.println("Trouble parsing : " + ioe.getMessage());
	    return null;
	}
	return elements;
    }

    public String format(Row row, int index) {
	return row.isNull(index) ? null : row.getString(index);
    }
}
