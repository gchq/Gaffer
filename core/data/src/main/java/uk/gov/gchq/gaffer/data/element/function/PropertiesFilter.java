/*
 * Copyright 2019 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicateComposite;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * An {@code PropertiesFilter} is a {@link Predicate} which evaluates a condition against
 * a provided {@link Properties} object.
 */
@JsonPropertyOrder(alphabetic = true)
public class PropertiesFilter extends TupleAdaptedPredicateComposite<String> {
    private final PropertiesTuple propertiesTuple = new PropertiesTuple();
    private boolean readOnly;

    public boolean test(final Properties properties) {
        propertiesTuple.setProperties(properties);
        return test(propertiesTuple);
    }

    public ValidationResult testWithValidationResult(final Properties properties) {
        final ValidationResult result = new ValidationResult();
        propertiesTuple.setProperties(properties);
        components.stream()
                .filter(predicate -> !predicate.test(propertiesTuple))
                .forEach(predicate -> result.addError(getErrorMsg(predicate)));
        return result;
    }

    private String getErrorMsg(final TupleAdaptedPredicate<String, ?> predicate) {
        final StringBuilder builder = new StringBuilder();
        builder.append("Filter: ")
                .append(predicate.getPredicate())
                .append(" returned false for properties: {");

        boolean firstProp = true;
        for (final String selection : predicate.getSelection()) {
            final Object value = propertiesTuple.get(selection);
            final String valueStr = null != value ? String.format("<%s>%s", value.getClass().getCanonicalName(), value) : "null";
            if (firstProp) {
                firstProp = false;
            } else {
                builder.append(", ");
            }
            builder.append(selection)
                    .append(": ")
                    .append(valueStr);
        }
        builder.append("}");

        return builder.toString();
    }

    @Override
    public List<TupleAdaptedPredicate<String, ?>> getComponents() {
        if (readOnly) {
            return Collections.unmodifiableList(super.getComponents());
        }

        return super.getComponents();
    }

    public void lock() {
        readOnly = true;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final PropertiesFilter that = (PropertiesFilter) obj;

        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(propertiesTuple, that.propertiesTuple)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(19, 53)
                .appendSuper(super.hashCode())
                .append(propertiesTuple)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("propertiesTuple", propertiesTuple)
                .toString();
    }

    public static class Builder {
        private final PropertiesFilter filter;

        public Builder() {
            this(new PropertiesFilter());
        }

        private Builder(final PropertiesFilter filter) {
            this.filter = filter;
        }

        public SelectedBuilder select(final String... selection) {
            final TupleAdaptedPredicate<String, Object> current = new TupleAdaptedPredicate<>();
            current.setSelection(selection);
            return new SelectedBuilder(filter, current);
        }

        public PropertiesFilter build() {
            return filter;
        }
    }

    public static final class SelectedBuilder {
        private final PropertiesFilter filter;
        private final TupleAdaptedPredicate<String, Object> current;

        private SelectedBuilder(final PropertiesFilter filter, final TupleAdaptedPredicate<String, Object> current) {
            this.filter = filter;
            this.current = current;
        }

        public Builder execute(final Predicate function) {
            current.setPredicate(function);
            filter.getComponents().add(current);
            return new Builder(filter);
        }
    }
}
