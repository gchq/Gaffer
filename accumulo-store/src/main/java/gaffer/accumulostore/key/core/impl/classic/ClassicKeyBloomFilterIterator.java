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

package gaffer.accumulostore.key.core.impl.classic;

import gaffer.accumulostore.key.core.AbstractCoreKeyBloomFilterIterator;
import gaffer.accumulostore.utils.ByteArrayEscapeUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import java.util.Arrays;

/**
 * The BloomFilterIterator should filter out elements based on their membership
 * of the provided bloomFilter. This implementation may not work as desired
 * depending on your {@link gaffer.accumulostore.key.AccumuloKeyPackage}
 * implementation.
 */
public class ClassicKeyBloomFilterIterator extends AbstractCoreKeyBloomFilterIterator {

    private boolean entity;
    private int pos = -1;

    @Override
    public boolean accept(final Key key, final Value value) {
        entity = true;
        byte[] keyData = key.getRowData().getBackingArray();
        boolean inSrc = filter.membershipTest(new org.apache.hadoop.util.bloom.Key(getSrcVertexFromKey(keyData)));
        if (entity || inSrc) {
            return true;
        }
        return filter.membershipTest(new org.apache.hadoop.util.bloom.Key(getDstVertexFromKey(keyData)));
    }

    protected byte[] getSrcVertexFromKey(final byte[] key) {
        for (int i = 0; i < key.length; ++i) {
            if (key[i] == ByteArrayEscapeUtils.DELIMITER) {
                pos = i;
                entity = false;
                break;
            }
        }
        if (!entity) {
            return Arrays.copyOf(key, pos);
        } else {
            return key;
        }
    }

    protected byte[] getDstVertexFromKey(final byte[] key) {
        return Arrays.copyOfRange(key, pos + 1, key.length - 2);
    }

}
