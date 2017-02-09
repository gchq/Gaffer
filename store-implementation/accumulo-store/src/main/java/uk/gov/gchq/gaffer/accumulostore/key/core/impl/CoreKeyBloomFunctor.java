/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.keyfunctor.KeyFunctor;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import java.util.Arrays;

public class CoreKeyBloomFunctor implements KeyFunctor {

    /**
     * Transforms a {@link org.apache.accumulo.core.data.Range} into a
     * BloomFilter key. If the first vertices in the start and end keys of the
     * range are the same, then we can create the appropriate BloomFilter key.
     * If the key does not correspond to either an
     * {@link uk.gov.gchq.gaffer.data.element.Entity} or an {@link uk.gov.gchq.gaffer.data.element.Edge}
     * then we return <code>null</code> to indicate that the range cannot be
     * converted into a single key for the Bloom filter.
     */
    @Override
    public org.apache.hadoop.util.bloom.Key transform(final Range range) {
        if (range.getStartKey() == null || range.getEndKey() == null) {
            return null;
        }
        final byte[] startKeyFirstIdentifier = getVertexFromRangeKey(
                range.getStartKey().getRowData().getBackingArray());
        final byte[] endKeyFirstIdentifier = getVertexFromRangeKey(range.getEndKey().getRowData().getBackingArray());
        if (Arrays.equals(startKeyFirstIdentifier, endKeyFirstIdentifier)) {
            return new org.apache.hadoop.util.bloom.Key(startKeyFirstIdentifier);
        }
        return null;
    }

    /**
     * Transforms an Accumulo {@link org.apache.accumulo.core.data.Key} into the
     * corresponding key for the Bloom filter. If the key does not correspond to
     * either an {@link uk.gov.gchq.gaffer.data.element.Entity} or an
     * {@link uk.gov.gchq.gaffer.data.element.Edge} then an {@link java.io.IOException} will
     * be thrown by the method which will be caught and then <code>null</code>
     * is returned.
     */
    @Override
    public org.apache.hadoop.util.bloom.Key transform(final Key key) {
        return new org.apache.hadoop.util.bloom.Key(getVertexFromRangeKey(key.getRowData().getBackingArray()));
    }

    public byte[] getVertexFromRangeKey(final byte[] key) {
        int pos = -1;
        for (int j = 0; j < key.length; ++j) {
            if (key[j] == ByteArrayEscapeUtils.DELIMITER) {
                pos = j;
                break;
            }
        }
        if (pos != -1) {
            return Arrays.copyOf(key, pos);
        } else {
            if (key[key.length - 1] != ByteArrayEscapeUtils.DELIMITER_PLUS_ONE) {
                return key;
            }
            if (getNumTrailingDelimPlusOne(key) % 2 == 0) {
                return key;
            }
            final byte[] bloomKey = new byte[key.length - 1];
            System.arraycopy(key, 0, bloomKey, 0, key.length - 1);
            return bloomKey;
        }
    }

    private static int getNumTrailingDelimPlusOne(final byte[] charArray) {
        int num = 0;
        for (int i = charArray.length - 1; i >= 0; i--) {
            if (charArray[i] == ByteArrayEscapeUtils.DELIMITER_PLUS_ONE) {
                num++;
            } else {
                break;
            }
        }
        return num;
    }
}
