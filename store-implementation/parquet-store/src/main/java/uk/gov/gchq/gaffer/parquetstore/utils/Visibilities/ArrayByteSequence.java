/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils.visibilities;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class ArrayByteSequence extends ByteSequence implements Serializable {
    private static final long serialVersionUID = 4850846929226802566L;
    protected byte[] data;
    protected int offset;
    protected int length;

    public ArrayByteSequence(final byte[] data) {
        this.data = data;
        this.offset = 0;
        this.length = data.length;
    }

    public ArrayByteSequence(final byte[] data, final int offset, final int length) {
        if (offset >= 0 && offset <= data.length && length >= 0 && offset + length <= data.length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
        } else {
            throw new IllegalArgumentException(" Bad offset and/or length data.length = " + data.length + " offset = " + offset + " length = " + length);
        }
    }

    public ArrayByteSequence(final String s) {
        this(s.getBytes(StandardCharsets.UTF_8));
    }

    public byte byteAt(final int i) {
        if (i < 0) {
            throw new IllegalArgumentException("i < 0, " + i);
        } else if (i >= this.length) {
            throw new IllegalArgumentException("i >= length, " + i + " >= " + this.length);
        } else {
            return this.data[this.offset + i];
        }
    }

    public byte[] getBackingArray() {
        return this.data;
    }

    public boolean isBackedByArray() {
        return true;
    }

    public int length() {
        return this.length;
    }

    public int offset() {
        return this.offset;
    }

    public byte[] toArray() {
        if (this.offset == 0 && this.length == this.data.length) {
            return this.data;
        } else {
            final byte[] copy = new byte[this.length];
            System.arraycopy(this.data, this.offset, copy, 0, this.length);
            return copy;
        }
    }

    public String toString() {
        return new String(this.data, this.offset, this.length, StandardCharsets.UTF_8);
    }
}
