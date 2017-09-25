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

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicateComposite;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * An {@code ElementFiler} is a {@link Predicate} which evaluates a condition against
 * a provided {@link Element} object.
 */
public class ElementFilter extends TupleAdaptedPredicateComposite<String> {
    private final ElementTuple elementTuple = new ElementTuple();
    private boolean readOnly;

    public boolean test(final Element element) {
        elementTuple.setElement(element);
        return test(elementTuple);
    }

    public ValidationResult testWithValidationResult(final Element element) {
        final ValidationResult result = new ValidationResult();
        elementTuple.setElement(element);
        components.stream()
                .filter(predicate -> !predicate.test(elementTuple))
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
            final Object value = elementTuple.get(selection);
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

        final ElementFilter that = (ElementFilter) obj;

        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(elementTuple, that.elementTuple)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(19, 53)
                .appendSuper(super.hashCode())
                .append(elementTuple)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("elementTuple", elementTuple)
                .toString();
    }

    public static class Builder {
        private final ElementFilter filter;

        public Builder() {
            this(new ElementFilter());
        }

        private Builder(final ElementFilter filter) {
            this.filter = filter;
        }

        public SelectedBuilder select(final String... selection) {
            final TupleAdaptedPredicate<String, Object> current = new TupleAdaptedPredicate<>();
            current.setSelection(selection);
            return new SelectedBuilder(filter, current);
        }

        public ElementFilter build() {
            return filter;
        }
    }

    public static final class SelectedBuilder {
        private final ElementFilter filter;
        private final TupleAdaptedPredicate<String, Object> current;

        private SelectedBuilder(final ElementFilter filter, final TupleAdaptedPredicate<String, Object> current) {
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
