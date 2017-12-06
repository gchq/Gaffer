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

import java.util.function.Function;

// To be removed after Koryphe 1.1.0
public class IterableFunction<I_ITEM, O_ITEM> implements Function<Iterable<I_ITEM>, Iterable<O_ITEM>> {
    private Function<I_ITEM, O_ITEM> delegateFunction;

    public IterableFunction(final Function... functions) {
        delegateFunction = functions[0];

        for (int i = 1; i < functions.length; i++) {
            delegateFunction = delegateFunction.andThen(functions[i]);
        }
    }

    @Override
    public Iterable<O_ITEM> apply(final Iterable<I_ITEM> items) {
        return IterableUtil.applyFunction(items, delegateFunction);
    }
}
