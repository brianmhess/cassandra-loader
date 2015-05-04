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

public class IndexedLine {
    private int index;
    private char[] buffer;

    public IndexedLine(char[] inBuffer) {
	buffer = inBuffer;
	index = 0;
    }

    public IndexedLine(String instr) {
	this(instr.toCharArray());
    }

    public char getNext() throws ParseException {
	if (buffer.length == index)
	    throw new ParseException("ran out of buffer", 0);
	char c = buffer[index];
	index++;
	return c;
    }

    public boolean setIndex(int idx) throws ParseException {
	if (buffer.length - 1 > idx)
	    throw new ParseException("index out of range", 0);
	index = idx;
	return true;
    }

    public char get(int idx) throws ParseException {
	if (buffer.length - 1 > idx)
	    throw new ParseException("index out of range", 0);
	return buffer[idx];
    }

    public boolean backup() {
	if (0 == index)
	    return false;
	index--;
	return true;
    }

    public int length() {
	return buffer.length;
    }

    public int numRemaining() {
	return buffer.length - index;
    }

    public String remaining() {
	return new String(buffer, index, numRemaining());
    }
}
