package gaffer.accumulostore.key.core.impl.byteEntity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import gaffer.accumulostore.key.core.impl.CoreKeyRangeElementPropertyFilterIterator;
import gaffer.accumulostore.utils.Constants;

public class ByteEntityRangeElementPropertyFilterIterator extends CoreKeyRangeElementPropertyFilterIterator {

	private boolean unDirectedEdges = false;
    private boolean directedEdges = false;
    private boolean incomingEdges = false;
    private boolean outgoingEdges = false;
	
    protected  boolean doAccept(final Key key, final Value value) {
    	boolean foundDelimiter = IsDelimiter(key);
        if(edges && entities) {
        	if(entities && foundDelimiter || edges && !foundDelimiter) {
        		return false;
        	}
        }
        if(entities && !foundDelimiter) {
        	return true; 
        }
        return checkEdge(key);
    }
    
    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        validateOptions(options);
        super.init(source, options, env);
    }

    private boolean checkEdge(final Key key) {
   	 	byte[] rowID = key.getRowData().getBackingArray();
        byte flag = rowID[rowID.length - 1];
        if (unDirectedEdges) {
            return flag == ByteEntityPositions.UNDIRECTED_EDGE;
        } else if (directedEdges) {
            return flag != ByteEntityPositions.UNDIRECTED_EDGE && checkDirection(flag);
        } else {
           return checkDirection(flag);
        }
    }

    private boolean checkDirection(final byte flag) {
    	if (incomingEdges) {
            if (flag == ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE) {
                return false;
            }
        } else if (outgoingEdges) {
            if (flag == ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        if (options.containsKey(Constants.DIRECTED_EDGE_ONLY) && options.containsKey(Constants.UNDIRECTED_EDGE_ONLY)) {
            throw new IllegalArgumentException("Must specify ONLY ONE of " + Constants.DIRECTED_EDGE_ONLY
                    + " or " + Constants.UNDIRECTED_EDGE_ONLY);
        }
        if (options.containsKey(Constants.INCOMING_EDGE_ONLY) && options.containsKey(Constants.OUTGOING_EDGE_ONLY)) {
            throw new IllegalArgumentException("Must specify ONLY ONE of " + Constants.INCOMING_EDGE_ONLY
                    + " or " + Constants.OUTGOING_EDGE_ONLY);
        }
        if (options.containsKey(Constants.INCOMING_EDGE_ONLY)) {
            incomingEdges = true;
        }
        if (options.containsKey(Constants.OUTGOING_EDGE_ONLY)) {
            outgoingEdges = true;
        }
        if (options.containsKey(Constants.DIRECTED_EDGE_ONLY)) {
            directedEdges = true;
        }
        if (options.containsKey(Constants.UNDIRECTED_EDGE_ONLY)) {
            unDirectedEdges = true;
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
