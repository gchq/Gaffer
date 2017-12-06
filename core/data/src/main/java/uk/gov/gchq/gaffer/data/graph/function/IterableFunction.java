/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.data.graph.function;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

// To be removed after Koryphe 1.1.0
public class IterableFunction<I_ITEM, O_ITEM> implements Function<Iterable<I_ITEM>, Iterable<O_ITEM>> {
    private List<Function> functions;

    public IterableFunction() {
    }

    public IterableFunction(final List<Function> functions) {
        this.functions = functions;
    }

    public List<Function> getFunctions() {
        return functions;
    }

    public void setFunctions(final List<Function> functions) {
        this.functions = functions;
    }

    @Override
    public Iterable<O_ITEM> apply(final Iterable<I_ITEM> items) {
        return IterableUtil.applyFunction(items, functions);
    }


    public static final class Builder<I_ITEM> {
        private final List<Function> functions = new ArrayList<>();

        public <O_ITEM> OutputBuilder<I_ITEM, O_ITEM> first(final Function<I_ITEM, O_ITEM> function) {
            return new OutputBuilder<>(function, functions);
        }
    }

    public static final class OutputBuilder<I_ITEM, O_ITEM> {
        private final List<Function> functions;

        private OutputBuilder(final Function<I_ITEM, O_ITEM> function, final List<Function> functions) {
            this.functions = functions;
            functions.add(function);
        }

        public <NEXT> OutputBuilder<I_ITEM, NEXT> then(final Function<? super O_ITEM, NEXT> function) {
            return new OutputBuilder(function, functions);
        }

        public IterableFunction<I_ITEM, O_ITEM> build() {
            return new IterableFunction<>(functions);
        }
    }
}
