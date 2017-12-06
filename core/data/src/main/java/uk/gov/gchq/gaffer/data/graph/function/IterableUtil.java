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

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

// TODO To be removed after Koryphe 1.1.0
public final class IterableUtil {
    private IterableUtil() {
        // Empty
    }

    public static <I_ITEM, O_ITEM> Iterable<O_ITEM> applyFunction(final Iterable<I_ITEM> input, final Function<I_ITEM, O_ITEM> function) {
        return () -> new Iterator<O_ITEM>() {
            Iterator<? extends I_ITEM> iterator = input.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public O_ITEM next() {
                return function.apply(iterator.next());
            }
        };
    }

    public static <I_ITEM, O_ITEM> Iterable<O_ITEM> applyFunctions(final Iterable<I_ITEM> input, final List<Function> functions) {
        return () -> new Iterator<O_ITEM>() {
            Iterator<? extends I_ITEM> iterator = input.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public O_ITEM next() {
                Object item = iterator.next();
                try {
                    for (final Function function : functions) {
                        if (null == function) {
                            throw new IllegalArgumentException("Function cannot be null");
                        }
                        item = function.apply(item);
                    }
                    return (O_ITEM) item;
                } catch (final ClassCastException c) {
                    throw new IllegalArgumentException("The input/output types of the functions were incompatible", c);
                }
            }
        };
    }
}
