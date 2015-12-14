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

import gaffer.predicate.typevalue.TypeValuePredicate;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link TypeValuePredicate} that indicates whether the value matches the provided Bloom
 * filter.
 */
public class ValueInBloomFilterPredicate implements TypeValuePredicate {

    private static final long serialVersionUID = -6559981116100008635L;
    private BloomFilter filter;

    public ValueInBloomFilterPredicate() { }

    public ValueInBloomFilterPredicate(BloomFilter filter) {
        this.filter = filter;
    }

    @Override
    public boolean accept(String type, String value) {
        return filter.membershipTest(new Key(value.getBytes()));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        filter.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        filter = new BloomFilter();
        filter.readFields(in);
    }

    @Override
    public String toString() {
        return "ValueInBloomFilterPredicate{" +
                "filter=" + filter +
                '}';
    }
}
