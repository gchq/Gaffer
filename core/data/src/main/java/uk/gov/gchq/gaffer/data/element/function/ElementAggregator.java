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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperatorComposite;
import java.util.function.BinaryOperator;

public class ElementAggregator extends TupleAdaptedBinaryOperatorComposite<String> {
    private final PropertiesTuple stateTuple = new PropertiesTuple();
    private final PropertiesTuple propertiesTuple = new PropertiesTuple();

    /**
     * Aggregates the element. Note - only the element properties are aggregated.
     * Aggregation requires elements to have the same identifiers and group.
     *
     * @param state   the other element to aggregate. This is normally the 'state' where the aggregated results will be set.
     * @param element the element to aggregated
     * @return Element - the aggregated element
     */
    public Element apply(final Element state, final Element element) {
        if (null == state) {
            return element;
        }

        apply(state.getProperties(), element.getProperties());
        return state;
    }

    public Properties apply(final Properties state, final Properties properties) {
        if (null == state) {
            return properties;
        }

        propertiesTuple.setProperties(properties);
        stateTuple.setProperties(state);
        apply(stateTuple, propertiesTuple);
        return state;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final ElementAggregator that = (ElementAggregator) obj;

        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(stateTuple, that.stateTuple)
                .append(propertiesTuple, that.propertiesTuple)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(stateTuple)
                .append(propertiesTuple)
                .toHashCode();
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
            final TupleAdaptedBinaryOperator<String, Object> current = new TupleAdaptedBinaryOperator<>();
            current.setSelection(selection);
            return new SelectedBuilder(aggregator, current);
        }

        public ElementAggregator build() {
            return aggregator;
        }
    }

    public static final class SelectedBuilder {
        private final ElementAggregator aggregator;
        private final TupleAdaptedBinaryOperator<String, Object> current;

        private SelectedBuilder(final ElementAggregator aggregator, final TupleAdaptedBinaryOperator<String, Object> current) {
            this.aggregator = aggregator;
            this.current = current;
        }

        public Builder execute(final BinaryOperator function) {
            current.setBinaryOperator(function);
            aggregator.getComponents().add(current);
            return new Builder(aggregator);
        }
    }
}
