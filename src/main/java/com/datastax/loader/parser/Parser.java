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
import java.io.StringReader;
import java.io.IOException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

// Parsing Interface - one method parse(String)
public interface Parser {
    public Object parse(String toparse) throws ParseException;
    public Object parse(IndexedLine il, String nullString, Character delim, 
                        Character escape, Character quote, boolean last)
        throws IOException, ParseException;
    public String format(Row row, int index) throws IndexOutOfBoundsException, InvalidTypeException;
    public String format(Object o);
}
