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
import com.datastax.driver.core.utils.Bytes;

// ByteBuffer parser
public class ByteBufferParser extends AbstractParser {
    public static enum BlobFormat {
        Base64("base64"),
        HexString("hex");

        private String formatStr;

        BlobFormat(String inFormatStr) {
            formatStr = inFormatStr;
        }

        public String getFormat() {
            return formatStr;
        }
    }

    public static BlobFormat getBlobFormat(String instr) {
        for (BlobFormat bs : BlobFormat.values()) {
            if (bs.getFormat().equalsIgnoreCase(instr)) {
                return bs;
            }
        }
        return null;
    }

    public static String getOptions() {
        String ret = "'" + BlobFormat.Base64.getFormat() + "'";
        ret = ret + ", '" + BlobFormat.HexString.getFormat() + "'";
        return ret;
    }

    private BlobFormat format;

    public ByteBufferParser(BlobFormat inFormat) {
        if (null == inFormat)
            inFormat = BlobFormat.Base64;
        this.format = inFormat;
    }

    public ByteBuffer parseIt(String toparse) {
        if (null == toparse)
            return null;

        switch (format) {
            case HexString:
                ByteBuffer bb = Bytes.fromHexString(toparse);
                return bb;

            default:
                byte[] barry = DatatypeConverter.parseBase64Binary(toparse);
                return ByteBuffer.wrap(barry);
        }
    }

    public String format(Object o) {
        ByteBuffer v = (ByteBuffer)o;
        switch (format) {
            case HexString:
                return Bytes.toHexString(v);

            default:
                return DatatypeConverter.printBase64Binary(v.array());
        }
    }
}
