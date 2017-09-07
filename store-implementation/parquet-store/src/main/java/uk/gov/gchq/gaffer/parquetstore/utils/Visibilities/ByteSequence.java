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

import org.apache.hadoop.io.WritableComparator;

public abstract class ByteSequence implements Comparable<ByteSequence> {
    public ByteSequence() {
    }

    public abstract byte byteAt(final int var1);

    public abstract int length();

    public abstract byte[] toArray();

    public abstract boolean isBackedByArray();

    public abstract byte[] getBackingArray();

    public abstract int offset();

    public static int compareBytes(final ByteSequence bs1, final ByteSequence bs2) {
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

    public int compareTo(final ByteSequence obs) {
        return this.isBackedByArray() && obs.isBackedByArray() ? WritableComparator.compareBytes(this.getBackingArray(), this.offset(), this.length(), obs.getBackingArray(), obs.offset(), obs.length()) : compareBytes(this, obs);
    }

    public boolean equals(final Object o) {
        if (o instanceof ByteSequence) {
            ByteSequence obs = (ByteSequence) o;
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
