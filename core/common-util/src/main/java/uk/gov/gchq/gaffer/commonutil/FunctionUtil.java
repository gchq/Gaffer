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
package uk.gov.gchq.gaffer.commonutil;

import java.util.function.Function;
import java.util.stream.Stream;

public class FunctionUtil {
    private FunctionUtil() {
        // Empty
    }

    public static <IN, MID, OUT> Function<IN, OUT> andThen(final Function<IN, MID> first, final Function<MID, ?>... functions) {
        FunctionBuilder<IN, MID> builder = new FunctionUtil.Builder<IN>()
                .first(first);

        Builder<IN> test = new FunctionUtil.Builder<>();



        Stream.of(functions).forEach(builder::then);

    }

//    public static <IN, OUT> Function<IN, OUT> andThen(final List<Function<?, ?>> functions) {
//        return andThen(functions.get(0), functions.toArray(new Function[0]));
//    }

    public static final class Builder<I> {
        public <NEXT> FunctionBuilder<I, NEXT> first(final Function<I, NEXT> function) {
            return new FunctionBuilder<>(function);
        }
    }

    public static final class FunctionBuilder<I, OUT> {
        private Function<I, OUT> function;

        private FunctionBuilder(final Function<I, OUT> function) {
            this.function = function;
        }

        public <NEXT> FunctionBuilder<I, NEXT> then(final Function<OUT, NEXT> function) {
            return new FunctionBuilder<>(this.function.andThen(function));
        }

        public Function<I, OUT> build() {
            return function;
        }
    }
}
