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

package uk.gov.gchq.koryphe.tuple.binaryoperator;

import uk.gov.gchq.koryphe.composite.Composite;
import uk.gov.gchq.koryphe.tuple.Tuple;
import java.util.function.BinaryOperator;

public class TupleAdaptedBinaryOperatorComposite extends Composite<TupleAdaptedBinaryOperator<String, ?>> implements BinaryOperator<Tuple<String>> {
    @Override
    public Tuple<String> apply(final Tuple<String> input, final Tuple<String> state) {
        Tuple<String> result = state;
        for (final TupleAdaptedBinaryOperator<String, ?> function : getFunctions()) {
            result = function.apply(input, result);
        }
        return result;
    }

    public static class Builder {
        private final TupleAdaptedBinaryOperatorComposite composite;

        public Builder() {
            this(new TupleAdaptedBinaryOperatorComposite());
        }

        private Builder(final TupleAdaptedBinaryOperatorComposite composite) {
            this.composite = composite;
        }

        public SelectedBuilder select(final String... selection) {
            final TupleAdaptedBinaryOperator<String, Object> current = new TupleAdaptedBinaryOperator<>();
            current.setSelection(selection);
            return new SelectedBuilder(composite, current);
        }

        public TupleAdaptedBinaryOperatorComposite build() {
            return composite;
        }
    }

    public static final class SelectedBuilder {
        private final TupleAdaptedBinaryOperatorComposite composite;
        private final TupleAdaptedBinaryOperator<String, Object> current;

        private SelectedBuilder(final TupleAdaptedBinaryOperatorComposite composite, final TupleAdaptedBinaryOperator<String, Object> current) {
            this.composite = composite;
            this.current = current;
        }

        public Builder execute(final BinaryOperator function) {
            current.setFunction(function);
            composite.getFunctions().add(current);
            return new Builder(composite);
        }
    }
}
