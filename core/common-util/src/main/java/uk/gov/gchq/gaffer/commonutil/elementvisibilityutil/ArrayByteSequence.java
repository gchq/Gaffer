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

package uk.gov.gchq.gaffer.commonutil.elementvisibilityutil;

import org.apache.hadoop.io.WritableComparator;

import uk.gov.gchq.gaffer.commonutil.ByteBufferUtil;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * This class is copied from org.apache.accumulo.core.data.ArrayByteSequence.
 */
public class ArrayByteSequence implements Serializable, Comparable<ArrayByteSequence> {
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

    public ArrayByteSequence(final ByteBuffer buffer) {
        if (buffer.hasArray()) {
            this.data = buffer.array();
            this.offset = buffer.position() + buffer.arrayOffset();
            this.length = buffer.remaining();
        } else {
            this.offset = 0;
            this.data = ByteBufferUtil.toBytes(buffer);
            this.length = data.length;
        }
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

    public ArrayByteSequence subSequence(final int start, final int end) {

        if (start > end || start < 0 || end > length) {
            throw new IllegalArgumentException("Bad start and/end start = " + start + " end=" + end + " offset=" + offset + " length=" + length);
        }

        return new ArrayByteSequence(data, offset + start, end - start);
    }

    public static int compareBytes(final ArrayByteSequence bs1, final ArrayByteSequence bs2) {
        int minLen = Math.min(bs1.length(), bs2.length());

        for (int i = 0; i < minLen; ++i) {
            int a = bs1.byteAt(i) & 255;
            int b = bs2.byteAt(i) & 255;
            if (a != b) {
                return a - b;
            }
        }

        return bs1.length() - bs2.length();
    }

    public int compareTo(final ArrayByteSequence obs) {
        return this.isBackedByArray() && obs.isBackedByArray() ? WritableComparator.compareBytes(this.getBackingArray(), this.offset(), this.length(), obs.getBackingArray(), obs.offset(), obs.length()) : compareBytes(this, obs);
    }

    public boolean equals(final Object o) {
        if (o instanceof ArrayByteSequence) {
            ArrayByteSequence obs = (ArrayByteSequence) o;
            return this == o ? true : (this.length() != obs.length() ? false : this.compareTo(obs) == 0);
        } else {
            return false;
        }
    }

    public int hashCode() {
        int hash = 1;
        if (this.isBackedByArray()) {
            byte[] i = this.getBackingArray();
            int end = this.offset() + this.length();

            for (int i1 = this.offset(); i1 < end; ++i1) {
                hash = 31 * hash + i[i1];
            }
        } else {
            for (int var5 = 0; var5 < this.length(); ++var5) {
                hash = 31 * hash + this.byteAt(var5);
            }
        }

        return hash;
    }
}
