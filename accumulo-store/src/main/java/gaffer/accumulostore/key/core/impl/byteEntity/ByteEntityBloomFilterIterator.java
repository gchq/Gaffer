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

package gaffer.accumulostore.key.core.impl.byteEntity;

import gaffer.accumulostore.key.core.AbstractCoreKeyBloomFilterIterator;
import gaffer.accumulostore.utils.ByteArrayEscapeUtils;
import gaffer.accumulostore.utils.Pair;

import java.util.Arrays;

/**
 * The BloomFilterIterator should filter out elements based on their membership
 * of the provided bloomFilter.
 */
public class ByteEntityBloomFilterIterator extends AbstractCoreKeyBloomFilterIterator {

    @Override
    protected Pair<byte[]> getVertices(final byte[] key) {
        int pos = -1;
        Pair<byte[]> vertices = new Pair<>();
        for (int i = 0; i < key.length; ++i) {
            if (key[i] == ByteArrayEscapeUtils.DELIMITER) {
                pos = i;
                break;
            }
        }
        if (key.length <= pos + 2) {
            return vertices;
        } else {
            vertices.setFirst(Arrays.copyOf(key, pos));
        }
        vertices.setSecond(Arrays.copyOfRange(key, pos + 3, key.length - 2));
        return vertices;
    }

}
