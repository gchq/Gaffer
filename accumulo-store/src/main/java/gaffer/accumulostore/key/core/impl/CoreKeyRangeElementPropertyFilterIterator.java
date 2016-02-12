/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore.key.core.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import gaffer.accumulostore.utils.ByteArrayEscapeUtils;
import gaffer.accumulostore.utils.Constants;

public abstract class CoreKeyRangeElementPropertyFilterIterator extends Filter {

    protected boolean edges = false;
    protected boolean entities = false;

    @Override
    public boolean accept(final Key key, final Value value) {
    	return doAccept(key, value);   
    }
    
    protected boolean IsDelimiter(final Key key) {
    	 byte[] rowID = key.getRowData().getBackingArray();
         boolean foundDelimiter = false;
         for (final byte aRowID : rowID) {
             if (aRowID == ByteArrayEscapeUtils.DELIMITER) {
                 foundDelimiter = true;
                 break;
             }
         }
		return foundDelimiter; 
    }
    
    protected abstract boolean doAccept(final Key key, final Value value);
    
    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        validateOptions(options);
        super.init(source, options, env);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) { 
        if (options.containsKey(Constants.ENTITY_ONLY)) {
            entities = true;
        }
        if(!options.containsKey(Constants.NO_EDGES)) {
        	edges = true;
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String, String> namedOptions = new HashMap<>();
        namedOptions.put(Constants.DIRECTED_EDGE_ONLY, "set if only want directed edges (value is ignored)");
        namedOptions.put(Constants.UNDIRECTED_EDGE_ONLY, "set if only want undirected edges (value is ignored)");
        return new IteratorOptions("EntityOrEdgeOnlyFilterIterator", "Only returns Entities or Edges as specified by the user's options",
                namedOptions, null);
    }

}
