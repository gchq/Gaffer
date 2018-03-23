/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.security.InvalidParameterException;

public class BytesAndRange {

    private byte[] bytes;
    private int length;
    private int offSet;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is a simple class to identify a range within the given byte array.")
    public BytesAndRange(final byte[] bytes, final int offSet, final int length) {
        this.bytes = bytes;
        if (length > -1 && offSet > -1) {
            this.length = length;
            this.offSet = offSet;
        } else {
            throw new InvalidParameterException("length and offset can't be negative");
        }
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "This is a simple class to identify a range within the given byte array.")
    public byte[] getBytes() {
        return bytes;
    }

    public int getLength() {
        return length;
    }

    public int getOffSet() {
        return offSet;
    }
}
