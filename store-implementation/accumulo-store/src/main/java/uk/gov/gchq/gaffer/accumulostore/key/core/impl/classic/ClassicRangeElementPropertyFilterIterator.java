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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorOptionsBuilder;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import java.io.IOException;
import java.util.Map;

public class ClassicRangeElementPropertyFilterIterator extends Filter {

    protected boolean edges = false;
    protected boolean entities = false;

    @Override
    public boolean accept(final Key key, final Value value) {
        final boolean foundDelimiter = hasDelimiter(key);
        if (!edges && foundDelimiter) {
            return false;
        } else if (!entities && !foundDelimiter) {
            return false;
        }
        return true;
    }

    protected boolean hasDelimiter(final Key key) {
        final byte[] rowID = key.getRowData().getBackingArray();
        boolean foundDelimiter = false;
        for (final byte aRowID : rowID) {
            if (aRowID == ByteArrayEscapeUtils.DELIMITER) {
                foundDelimiter = true;
                break;
            }
        }
        return foundDelimiter;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options,
            final IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        validateOptions(options);
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (!super.validateOptions(options)) {
            return false;
        }
        if (options.containsKey(AccumuloStoreConstants.INCLUDE_ENTITIES)) {
            entities = true;
        }
        if (!options.containsKey(AccumuloStoreConstants.NO_EDGES)) {
            edges = true;
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(super.describeOptions())
                .addNamedOption(AccumuloStoreConstants.INCLUDE_ENTITIES, "Optional: Set if entities should be returned")
                .addNamedOption(AccumuloStoreConstants.NO_EDGES, "Optional: Set if no edges should be returned")
                .setIteratorName(AccumuloStoreConstants.RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME)
                .setIteratorDescription(
                        "Only returns Entities or Edges that are directed undirected incoming or outgoing as specified by the user's options")
                .build();
    }

}
