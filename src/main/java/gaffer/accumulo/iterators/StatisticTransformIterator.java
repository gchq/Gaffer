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
import gaffer.accumulo.ConversionUtils;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.transform.StatisticsTransform;
import gaffer.utils.WritableToStringConverter;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.*;

/**
 * An iterator that applies the provided {@link StatisticsTransform}.
 */
public class StatisticTransformIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {

    private SortedKeyValueIterator<Key, Value> source;
    private StatisticsTransform transform;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        validateOptions(options);
    }

    @Override
    public boolean hasTop() {
        return this.source.hasTop();
    }

    @Override
    public void next() throws IOException {
        this.source.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.source.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {
        return this.source.getTopKey();
    }

    @Override
    public Value getTopValue() {
        try {
            Value value = this.source.getTopValue();
            SetOfStatistics statistics = ConversionUtils.getSetOfStatisticsFromValue(value);
            statistics = transform.transform(statistics);
            return ConversionUtils.getValueFromSetOfStatistics(statistics);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        StatisticTransformIterator statisticTransformIterator = new StatisticTransformIterator();
        statisticTransformIterator.source = source.deepCopy(env);
        statisticTransformIterator.transform = transform;
        return statisticTransformIterator;
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String, String> namedOptions = new HashMap<String, String>();
        namedOptions.put(Constants.STATISTIC_TRANSFORM,
                "The desired statistics transform serialised to a string (using the method serialiseTransformToString in StatisticTransform).");
        IteratorOptions iteratorOptions =  new IteratorOptions(Constants.STATISTIC_TRANSFORM_ITERATOR_NAME,
                "Transforms statistics", namedOptions, null);
        return iteratorOptions;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        // Check correct option exists
        if (!options.containsKey(Constants.STATISTIC_TRANSFORM)) {
            throw new IllegalArgumentException("Must specify the " + Constants.STATISTIC_TRANSFORM);
        }
        // Deserialise transform
        try {
            transform = (StatisticsTransform) WritableToStringConverter.deserialiseFromString(options.get(Constants.STATISTIC_TRANSFORM));
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to deserialise a StatisticsTransform from the string: " + options.get(Constants.STATISTIC_TRANSFORM));
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Unable to deserialise a StatisticsTransform from the string: " + options.get(Constants.STATISTIC_TRANSFORM));
        }
        return true;
    }

}
