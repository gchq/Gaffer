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
import uk.gov.gchq.koryphe.composite.Composite;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.bifunction.TupleAdaptedBiFunction;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public class ElementAggregator extends Composite<TupleAdaptedBiFunction<String, ?, ?>> implements BinaryOperator<Tuple<String>> {
    private final PropertiesTuple propertiesTuple = new PropertiesTuple();
    private final PropertiesTuple stateTuple = new PropertiesTuple();

    /**
     * Aggregates the element. Note - only the element properties are aggregated.
     * Aggregation requires elements to have the same identifiers and group.
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

    @Override
    public Tuple<String> apply(final Tuple<String> input, final Tuple<String> state) {
        Tuple<String> result = state;
        for (TupleAdaptedBiFunction<String, ?, ?> function : getFunctions()) {
            result = function.apply(input, result);
        }
        return result;
    }

    public static class Builder {
        private final ElementAggregator aggregator;
        private TupleAdaptedBiFunction<String, Object, Object> currentFunction = new TupleAdaptedBiFunction<>();
        private boolean selected;
        private boolean executed;

        public Builder() {
            this(new ElementAggregator());
        }

        public Builder(final ElementAggregator aggregator) {
            this.aggregator = aggregator;
        }

        public Builder select(final String... selection) {
            if (selected) {
                aggregator.getFunctions().add(currentFunction);
                currentFunction = new TupleAdaptedBiFunction<>();
                selected = false;
            }
            currentFunction.setSelection(selection);
            selected = true;
            return this;
        }

        public Builder execute(final BiFunction function) {
            if (executed) {
                aggregator.getFunctions().add(currentFunction);
                currentFunction = new TupleAdaptedBiFunction<>();
                executed = false;
            }
            currentFunction.setFunction(function);
            executed = true;
            return this;
        }

        public ElementAggregator build() {
            if (executed || selected) {
                aggregator.getFunctions().add(currentFunction);
                currentFunction = new TupleAdaptedBiFunction<>();
                selected = false;
                executed = false;
            }
            return aggregator;
        }
    }
}
