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

package uk.gov.gchq.gaffer.data.element.koryphe;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.koryphe.composite.Composite;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.function.TuplePredicate;
import uk.gov.gchq.koryphe.tuple.mask.TupleMask;
import uk.gov.gchq.koryphe.tuple.n.mask.TupleMaskN;
import java.util.function.Predicate;

public class ElementFilter extends Composite<TuplePredicate<String, ?>> implements Predicate<Tuple<String>> {
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
        private TuplePredicate currentFunction = new TuplePredicate();
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
                filter.getFunctions().add(currentFunction);
                currentFunction = new TuplePredicate();
                selected = false;
            }
            final TupleMask tupleMask = new TupleMaskN<>();
            tupleMask.setFields(selection);
            currentFunction.setSelection(tupleMask);
            selected = true;
            return this;
        }

        public Builder execute(final Predicate function) {
            if (executed) {
                filter.getFunctions().add(currentFunction);
                currentFunction = new TuplePredicate();
                executed = false;
            }
            currentFunction.setFunction(function);
            executed = true;
            return this;
        }

        public ElementFilter build() {
            if (executed || selected) {
                filter.getFunctions().add(currentFunction);
                currentFunction = new TuplePredicate();
                selected = false;
                executed = false;
            }
            return filter;
        }
    }
}
