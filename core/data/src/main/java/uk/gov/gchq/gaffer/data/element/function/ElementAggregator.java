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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.PropertiesTuple;
import uk.gov.gchq.gaffer.function.AggregateFunction;
import uk.gov.gchq.gaffer.function.processor.Aggregator;

/**
 * Element Aggregator - for aggregating {@link uk.gov.gchq.gaffer.data.element.Element}s.
 * When Elements are aggregated it is only the Element {@link uk.gov.gchq.gaffer.data.element.Properties} that are aggregated.
 * Aggregation requires elements to have the same identifiers and group.
 * <p>
 * Use {@link uk.gov.gchq.gaffer.data.element.function.ElementAggregator.Builder} to build an ElementAggregator.
 * <p>
 * To use this aggregator:
 * <ul>
 * <li>Use {@link uk.gov.gchq.gaffer.data.element.function.ElementAggregator.Builder} to build an ElementAggregator</li>
 * <li>first group Elements with the same identifiers and group together</li>
 * <li>call initFunctions()</li>
 * <li>for each element in the group call aggregate(element)</li>
 * <li>create a new element then call state(newElement) to populate the new element with the aggregated properties</li>
 * </ul>
 *
 * @see uk.gov.gchq.gaffer.data.element.function.ElementAggregator.Builder
 * @see uk.gov.gchq.gaffer.function.processor.Aggregator
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ElementAggregator that = (ElementAggregator) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(functions, that.functions)
                .append(initialised, that.initialised)
                .append(propertiesTuple, that.propertiesTuple)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(functions)
                .append(initialised)
                .append(propertiesTuple)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("functions", functions)
                .append("initialised", initialised)
                .append("propertiesTuple", propertiesTuple)
                .toString();
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
