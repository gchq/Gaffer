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
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorOptionsBuilder;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import java.util.Iterator;
import java.util.Set;

public class CoreKeyGroupByAggregatorIterator extends CoreKeyGroupByCombiner {

    @Override
    public Properties reduce(final String group, final Key key, final Iterator<Properties> iter, final Set<String> groupBy, final ElementAggregator viewAggregator) {
        if (!iter.hasNext()) {
            return new Properties();
        }

        final Properties properties = iter.next();
        if (!iter.hasNext()) {
            return properties;
        }

        final ElementAggregator aggregator = schema.getElement(group).getQueryAggregator(groupBy, viewAggregator);
        Properties aggregatedProps = properties;
        while (iter.hasNext()) {
            aggregatedProps = aggregator.apply(aggregatedProps, iter.next());
        }

        return aggregatedProps;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptionsBuilder(super.describeOptions())
                .addSchemaNamedOption()
                .addElementConverterClassNamedOption()
                .build();
    }
}
