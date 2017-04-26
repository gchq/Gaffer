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

package uk.gov.gchq.koryphe.tuple.predicate;

import uk.gov.gchq.koryphe.composite.Composite;
import uk.gov.gchq.koryphe.tuple.Tuple;
import java.util.function.Predicate;

public class TupleAdaptedPredicateComposite extends Composite<TupleAdaptedPredicate<String, ?>> implements Predicate<Tuple<String>> {
    @Override
    public boolean test(final Tuple<String> input) {
        for (final TupleAdaptedPredicate<String, ?> predicate : getFunctions()) {
            if (!predicate.test(input)) {
                return false;
            }
        }
        return true;
    }

    public static class Builder {
        private final TupleAdaptedPredicateComposite composite;

        public Builder() {
            this(new TupleAdaptedPredicateComposite());
        }

        private Builder(final TupleAdaptedPredicateComposite composite) {
            this.composite = composite;
        }

        public SelectedBuilder select(final String... selection) {
            final TupleAdaptedPredicate<String, Object> current = new TupleAdaptedPredicate<>();
            current.setSelection(selection);
            return new SelectedBuilder(composite, current);
        }

        public TupleAdaptedPredicateComposite build() {
            return composite;
        }
    }

    public static final class SelectedBuilder {
        private final TupleAdaptedPredicateComposite filter;
        private final TupleAdaptedPredicate<String, Object> current;

        private SelectedBuilder(final TupleAdaptedPredicateComposite filter, final TupleAdaptedPredicate<String, Object> current) {
            this.filter = filter;
            this.current = current;
        }

        public Builder execute(final Predicate function) {
            current.setFunction(function);
            filter.getFunctions().add(current);
            return new Builder(filter);
        }
    }
}
