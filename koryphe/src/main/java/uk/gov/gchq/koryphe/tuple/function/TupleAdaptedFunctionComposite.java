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

package uk.gov.gchq.koryphe.tuple.function;

import uk.gov.gchq.koryphe.composite.Composite;
import uk.gov.gchq.koryphe.tuple.Tuple;
import java.util.function.Function;

public class TupleAdaptedFunctionComposite
        extends Composite<TupleAdaptedFunction<String, ?, ?>>
        implements Function<Tuple<String>, Tuple<String>> {
    @Override
    public Tuple<String> apply(final Tuple<String> input) {
        Tuple<String> result = input;
        for (final TupleAdaptedFunction<String, ?, ?> function : getFunctions()) {
            // Assume the output of one is the input of the next
            result = function.apply(result);
        }
        return result;
    }

    public static class Builder {
        private final TupleAdaptedFunctionComposite transformer;

        public Builder() {
            this(new TupleAdaptedFunctionComposite());
        }

        private Builder(final TupleAdaptedFunctionComposite transformer) {
            this.transformer = transformer;
        }

        public SelectedBuilder select(final String... selection) {
            final TupleAdaptedFunction<String, Object, Object> current = new TupleAdaptedFunction<>();
            current.setSelection(selection);
            return new SelectedBuilder(transformer, current);
        }

        public TupleAdaptedFunctionComposite build() {
            return transformer;
        }
    }

    public static final class SelectedBuilder {
        private final TupleAdaptedFunctionComposite transformer;
        private final TupleAdaptedFunction<String, Object, Object> current;

        private SelectedBuilder(final TupleAdaptedFunctionComposite transformer, final TupleAdaptedFunction<String, Object, Object> current) {
            this.transformer = transformer;
            this.current = current;
        }

        public ExecutedBuilder execute(final Function function) {
            current.setFunction(function);
            return new ExecutedBuilder(transformer, current);
        }
    }

    public static final class ExecutedBuilder {
        private final TupleAdaptedFunctionComposite transformer;
        private final TupleAdaptedFunction<String, Object, Object> current;

        private ExecutedBuilder(final TupleAdaptedFunctionComposite transformer, final TupleAdaptedFunction<String, Object, Object> current) {
            this.transformer = transformer;
            this.current = current;
        }

        public Builder project(final String... projection) {
            current.setProjection(projection);
            transformer.getFunctions().add(current);
            return new Builder(transformer);
        }
    }
}
