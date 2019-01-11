/*
 * Copyright (c) YugaByte, Inc.
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

import java.text.*;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

// Json parser - simple
public class JsonParser extends AbstractParser {
    JSONParser simpleJsonParser;

    public JsonParser() {
      simpleJsonParser = new JSONParser();
    }

    public Object parseIt(String toparse) throws java.text.ParseException {
      try {
        // Parse just to make sure that the Json string is well formed.
        simpleJsonParser.parse(toparse);
        // We expect json to be bound as a string. Not JSONObject.
        return toparse;
      } catch (org.json.simple.parser.ParseException e) {
        throw new java.text.ParseException(e.getMessage(), e.getPosition());
      }
    }

    public String format(Object o) {
        return (String)o;
    }
}
