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
import java.lang.StringBuilder;
import java.lang.IndexOutOfBoundsException;
import java.io.StringReader;
import java.io.IOException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

// String parser - simple
public class StringParser extends AbstractParser {
    public String parse(String toparse) {
	String ret = toparse.replaceAll("\\n", "\n");
	return ret;
    }

    public String format(Object o) {
	String iv = (String)o;
	String v = iv.replaceAll("\\n", "\\\\n").replaceAll("\"", "\\\"");
	String ret = "\"" + v + "\"";
	return ret;
    }
}
