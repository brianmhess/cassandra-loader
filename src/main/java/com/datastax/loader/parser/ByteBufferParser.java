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

import java.nio.ByteBuffer;
import javax.xml.bind.DatatypeConverter;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

// ByteBuffer parser
public class ByteBufferParser extends AbstractParser<ByteBuffer> {
    public ByteBuffer parseIt(String toparse) {
        if (null == toparse)
            return null;
        byte[] barry = DatatypeConverter.parseBase64Binary(toparse);
        return ByteBuffer.wrap(barry);
    }

    public String format(Object o) {
        ByteBuffer v = (ByteBuffer)o;
        return DatatypeConverter.printBase64Binary(v.array());
    }
}
