/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.predicate.typevalue.impl;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;

import java.io.*;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test of {@link ValueInBloomFilterPredicate}. Tests the <code>accept()</code>, <code>equals()</code>,
 * <code>write()</code> and <code>readFields()</code> methods.
 */
public class TestValueInBloomFilterPredicate {

    @Test
    public void testAccept() {
        BloomFilter filter = new BloomFilter(100, 5, Hash.MURMUR_HASH);
        filter.add(new Key("ABC".getBytes()));
        filter.add(new Key("DEF".getBytes()));
        ValueInBloomFilterPredicate predicate = new ValueInBloomFilterPredicate(filter);
        assertTrue(predicate.accept("X", "ABC"));
        assertTrue(predicate.accept("X", "DEF"));
        assertFalse(predicate.accept("Y", "lkjhgfdsa"));
    }

    @Test
    public void testWriteRead() throws IOException {
        BloomFilter filter = new BloomFilter(100, 5, Hash.MURMUR_HASH);
        filter.add(new Key("ABC".getBytes()));
        filter.add(new Key("DEF".getBytes()));
        ValueInBloomFilterPredicate predicate = new ValueInBloomFilterPredicate(filter);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        ValueInBloomFilterPredicate read = new ValueInBloomFilterPredicate();
        read.readFields(in);

        assertTrue(read.accept("X", "ABC"));
        assertTrue(read.accept("X", "DEF"));
        assertFalse(read.accept("Y", "lkjhgfdsa"));
    }

}
