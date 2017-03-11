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
import uk.gov.gchq.koryphe.composite.Composite;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;
import java.util.function.Predicate;

public class ElementFilter extends Composite<TupleAdaptedPredicate<String, ?>> implements Predicate<Tuple<String>> {
    private final ElementTuple elementTuple = new ElementTuple();

    public boolean test(final Element element) {
        elementTuple.setElement(element);
        return test(elementTuple);
    }

    @Override
    public boolean test(final Tuple<String> input) {
        for (Predicate<Tuple<String>> predicate : getFunctions()) {
            if (!predicate.test(input)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builder for {@link ElementFilter}.
     */
    public static class Builder {
        private final ElementFilter filter;
        private TupleAdaptedPredicate<String, Object> current = new TupleAdaptedPredicate<>();
        private boolean selected;
        private boolean executed;

        public Builder() {
            this(new ElementFilter());
        }

        public Builder(final ElementFilter filter) {
            this.filter = filter;
        }

        public Builder select(final String... selection) {
            if (selected) {
                addCurrent();
            }
            current.setSelection(selection);
            selected = true;
            return this;
        }

        public Builder execute(final Predicate function) {
            if (executed) {
                addCurrent();
            }
            current.setFunction(function);
            executed = true;
            return this;
        }

        public ElementFilter build() {
            if (executed || selected) {
                addCurrent();
            }
            return filter;
        }

        protected void addCurrent() {
            if (null == current.getSelection()) {
                throw new IllegalArgumentException("Must set both selection and function. Missing selection.");
            }

            if (null == current.getFunction()) {
                throw new IllegalArgumentException("Must set both selection and function. Missing function.");
            }

            filter.getFunctions().add(current);
            current = new TupleAdaptedPredicate<>();
            selected = false;
            executed = false;
        }
    }
}
