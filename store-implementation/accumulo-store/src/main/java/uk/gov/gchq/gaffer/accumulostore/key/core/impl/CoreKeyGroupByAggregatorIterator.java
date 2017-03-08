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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AggregationException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorOptionsBuilder;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

public class CoreKeyGroupByAggregatorIterator extends CoreKeyGroupByCombiner {

    @Override
    public Properties reduce(final String group, final Key key, final Iterator<Properties> iter) {
        if (!iter.hasNext()) {
            return new Properties();
        }

        final Properties properties = iter.next();
        if (!iter.hasNext()) {
            return properties;
        }

        final ElementAggregator aggregator = schema.getElement(group).getAggregator();
        aggregator.aggregate(properties);
        while (iter.hasNext()) {
            aggregator.aggregate(iter.next());
        }

        final Properties aggregatedProperties = new Properties();
        aggregator.state(aggregatedProperties);

        return aggregatedProperties;
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

        try {
            final Class<?> elementConverterClass = Class
                    .forName(options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));
            elementConverter = (AccumuloElementConverter) elementConverterClass.getConstructor(Schema.class)
                    .newInstance(schema);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new AggregationException("Failed to load element converter from class name provided : "
                    + options.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS), e);
        }
        return true;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(super.describeOptions())
                .addSchemaNamedOption()
                .addElementConverterClassNamedOption()
                .build();
    }
}
