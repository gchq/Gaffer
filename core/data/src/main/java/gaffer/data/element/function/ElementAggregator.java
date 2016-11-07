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

package gaffer.data.element.function;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.element.Element;
import gaffer.data.element.Properties;
import gaffer.data.element.PropertiesTuple;
import gaffer.function.AggregateFunction;
import gaffer.function.processor.Aggregator;

/**
 * Element Aggregator - for aggregating {@link gaffer.data.element.Element}s.
 * When Elements are aggregated it is only the Element {@link gaffer.data.element.Properties} that are aggregated.
 * Aggregation requires elements to have the same identifiers and group.
 * <p>
 * Use {@link gaffer.data.element.function.ElementAggregator.Builder} to build an ElementAggregator.
 * <p>
 * To use this aggregator:
 * <ul>
 * <li>Use {@link gaffer.data.element.function.ElementAggregator.Builder} to build an ElementAggregator</li>
 * <li>first group Elements with the same identifiers and group together</li>
 * <li>call initFunctions()</li>
 * <li>for each element in the group call aggregate(element)</li>
 * <li>create a new element then call state(newElement) to populate the new element with the aggregated properties</li>
 * </ul>
 *
 * @see gaffer.data.element.function.ElementAggregator.Builder
 * @see gaffer.function.processor.Aggregator
 */
public class ElementAggregator extends Aggregator<String> {
    private final PropertiesTuple propertiesTuple = new PropertiesTuple();

    /**
     * Aggregates the element. Note - only the element properties are aggregated.
     * Aggregation requires elements to have the same identifiers and group.
     *
     * @param element the element to be aggregated.
     */
    public void aggregate(final Element element) {
        aggregate(element.getProperties());
    }

    public void aggregate(final Properties properties) {
        propertiesTuple.setProperties(properties);
        super.aggregate(propertiesTuple);
    }

    /**
     * Calls state on the element. Note - state is actually only called on the element properties.
     * Aggregation requires elements to have the same identifiers and group.
     *
     * @param element the element to be aggregated.
     */
    public void state(final Element element) {
        state(element.getProperties());
    }

    public void state(final Properties properties) {
        propertiesTuple.setProperties(properties);
        super.state(propertiesTuple);
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Uses super.cloneFunctions instead for better performance")
    @Override
    public ElementAggregator clone() {
        final ElementAggregator clone = new ElementAggregator();
        clone.addFunctions(super.cloneFunctions());

        return clone;
    }

    /**
     * Builder for {@link ElementAggregator}.
     */
    public static class Builder extends Aggregator.Builder<String> {
        public Builder() {
            this(new ElementAggregator());
        }

        public Builder(final ElementAggregator aggregator) {
            super(aggregator);
        }

        public Builder select(final String... selection) {
            return (Builder) super.select(selection);
        }

        public Builder execute(final AggregateFunction function) {
            return (Builder) super.execute(function);
        }

        public ElementAggregator build() {
            return (ElementAggregator) super.build();
        }
    }
}
