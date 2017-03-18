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

package uk.gov.gchq.gaffer.data.element.function;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.koryphe.tuple.bifunction.TupleAdaptedBiFunction;
import uk.gov.gchq.koryphe.tuple.bifunction.TupleAdaptedBiFunctionComposite;
import java.util.function.BiFunction;

public class ElementAggregator extends TupleAdaptedBiFunctionComposite {
    private final PropertiesTuple propertiesTuple = new PropertiesTuple();
    private final PropertiesTuple stateTuple = new PropertiesTuple();

    /**
     * Aggregates the element. Note - only the element properties are aggregated.
     * Aggregation requires elements to have the same identifiers and group.
     *
     * @param element the element to aggregated
     * @param state   the other element to aggregate. This is normally the 'state' where the aggregated results will be set.
     * @return Element - the aggregated element
     */
    public Element apply(final Element element, final Element state) {
        if (null == state) {
            return element;
        }

        apply(element.getProperties(), state.getProperties());
        return state;
    }

    public Properties apply(final Properties properties, final Properties state) {
        if (null == state) {
            return properties;
        }

        propertiesTuple.setProperties(properties);
        stateTuple.setProperties(state);
        apply(propertiesTuple, stateTuple);
        return state;
    }

    public static class Builder {
        private final ElementAggregator aggregator;

        public Builder() {
            this(new ElementAggregator());
        }

        private Builder(final ElementAggregator aggregator) {
            this.aggregator = aggregator;
        }

        public SelectedBuilder select(final String... selection) {
            final TupleAdaptedBiFunction<String, Object, Object> current = new TupleAdaptedBiFunction<>();
            current.setSelection(selection);
            return new SelectedBuilder(aggregator, current);
        }

        public ElementAggregator build() {
            return aggregator;
        }
    }

    public static final class SelectedBuilder {
        private final ElementAggregator aggregator;
        private final TupleAdaptedBiFunction<String, Object, Object> current;

        private SelectedBuilder(final ElementAggregator aggregator, final TupleAdaptedBiFunction<String, Object, Object> current) {
            this.aggregator = aggregator;
            this.current = current;
        }

        public Builder execute(final BiFunction function) {
            current.setFunction(function);
            aggregator.getFunctions().add(current);
            return new Builder(aggregator);
        }
    }
}
