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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import gaffer.accumulostore.key.core.impl.CoreKeyRangeElementPropertyFilterIterator;
import gaffer.accumulostore.utils.Constants;

public class ClassicRangeElementPropertyFilterIterator extends CoreKeyRangeElementPropertyFilterIterator {


    @Override
    public boolean doAccept(final Key key, final Value value) {
        final boolean foundDelimiter = isDelimiter(key);
        if (!edges && foundDelimiter) {
            return false;
        } else if (!entities && !foundDelimiter) {
            return false;
        }
        return true;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        validateOptions(options);
        super.init(source, options, env);
    }

    @Override
    public boolean validateOptions(final Map<String, String> options) {
        if (options.containsKey(Constants.INCLUDE_ENTITIES)) {
            entities = true;
        }
        if (!options.containsKey(Constants.NO_EDGES)) {
            edges = true;
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        final Map<String, String> namedOptions = new HashMap<>();
        namedOptions.put(Constants.DIRECTED_EDGE_ONLY, "set if only want directed edges (value is ignored)");
        namedOptions.put(Constants.UNDIRECTED_EDGE_ONLY, "set if only want undirected edges (value is ignored)");
        return new IteratorOptions("EntityOrEdgeOnlyFilterIterator", "Only returns Entities or Edges as specified by the user's options",
                namedOptions, null);
    }

}
