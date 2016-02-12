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
        boolean foundDelimiter = IsDelimiter(key);
        if(edges && entities) {
        	if(entities && foundDelimiter || edges && !foundDelimiter) {
        		return false;
        	}
        }
        return true;
    }
        
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
