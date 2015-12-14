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
package gaffer.accumulo.iterators;

import gaffer.accumulo.Constants;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.predicate.Predicate;
import gaffer.utils.WritableToStringConverter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Removes all rows that do not match the provided {@link Predicate} of {@link RawGraphElementWithStatistics}. This is
 * achieved by creating a {@link RawGraphElementWithStatistics} from the Accumulo {@link Key} and {@link Value}, and then
 * testing that against the provided predicate.
 */
public class RawElementWithStatsFilter extends Filter {

    private Predicate<RawGraphElementWithStatistics> predicate;

    @Override
    public boolean accept(Key key, Value value) {
        try {
            return predicate.accept(new RawGraphElementWithStatistics(key, value));
        } catch (IOException e) {
            // Shouldn't happen - if it does ignore this key-value pair
            return false;
        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        validateOptions(options);
        super.init(source, options, env);
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        // Check correct option exists
        if (!options.containsKey(Constants.RAW_ELEMENT_WITH_STATS_PREDICATE)) {
            throw new IllegalArgumentException("Must specify the " + Constants.RAW_ELEMENT_WITH_STATS_PREDICATE);
        }
        // Deserialise predicate
        String serialisedPredicate = options.get(Constants.RAW_ELEMENT_WITH_STATS_PREDICATE);
        try {
            predicate = (Predicate<RawGraphElementWithStatistics>) WritableToStringConverter.deserialiseFromString(serialisedPredicate);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't deserialise the predicate " + e.getMessage());
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Can't deserialise the predicate " + e.getMessage());
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String, String> namedOptions = new HashMap<String, String>();
        namedOptions.put(Constants.RAW_ELEMENT_WITH_STATS_PREDICATE, "A RawGraphElementWithStatisticsPredicate serialised to a String");
        IteratorOptions iteratorOptions =  new IteratorOptions(Constants.RAW_ELEMENT_WITH_STATS_PREDICATE_FILTER_NAME,
                "Only returns rows that match the supplied RawGraphElementWithStatisticsPredicate",
                namedOptions, null);
        return iteratorOptions;
    }
}
