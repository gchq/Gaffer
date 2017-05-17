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
package uk.gov.gchq.gaffer.commonutil.stream;

import uk.gov.gchq.gaffer.commonutil.collection.LimitedSortedSet;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Java 8 {@link java.util.stream.Collector}s for Gaffer, based on the {@link java.util.stream.Collectors}
 * class.
 */
public final class GafferCollectors {

    private GafferCollectors() {
        // Empty
    }

    /**
     * Returns a {@link java.util.stream.Collector} that accumulates the input
     * elements into a {@link java.util.List}, before wrapping the list in a
     * {@link uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable}.
     *
     * @param <T> the type of the input elements
     * @return a {@link java.util.stream.Collector} which collects all the input
     * elements into a {@link uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable}
     */
    public static <T> Collector<T, List<T>, CloseableIterable<T>> toCloseableIterable() {
        return new GafferCollectorImpl<>(
                ArrayList::new,
                List::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                WrappedCloseableIterable::new
        );
    }

    /**
     * Returns a {@link java.util.stream.Collector} that accumulates the input
     * elements into a {@link java.util.LinkedHashSet}.
     *
     * @param <T> the type of the input elements
     * @return a {@link java.util.stream.Collector} which collects all the input
     * elements into a {@link java.util.LinkedHashSet}
     */
    public static <T> Collector<T, ?, Set<T>> toLinkedHashSet() {
        return new GafferCollectorImpl<>(
                (Supplier<Set<T>>) LinkedHashSet::new,
                Set::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                },
                set -> set
        );
    }

    public static <T> Collector<T, Set<T>, SortedSet<T>> toSortedSet(final Comparator<T> comparator) {
        return new GafferCollectorImpl<>(
                () -> new TreeSet<>(comparator),
                Collection::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                }
        );
    }

    public static <T> Collector<T, Set<T>, LimitedSortedSet<T>> toLimitedSortedSet(final Comparator<T> comparator, final int limit) {
        return new GafferCollectorImpl<>(
                () -> new LimitedSortedSet<>(comparator, limit),
                Collection::add,
                (left, right) -> {
                    left.addAll(right);
                    return left;
                }
        );
    }

    /**
     * Simple implementation class for {@code GafferCollector}.
     *
     * @param <T> the type of elements to be collected
     * @param <R> the type of the result
     */
    static class GafferCollectorImpl<T, A, R> implements Collector<T, A, R> {
        private final Supplier<A> supplier;
        private final BiConsumer<A, T> accumulator;
        private final BinaryOperator<A> combiner;
        private final Function<A, R> finisher;

        GafferCollectorImpl(final Supplier<A> supplier,
                            final BiConsumer<A, T> accumulator,
                            final BinaryOperator<A> combiner,
                            final Function<A, R> finisher) {
            this.supplier = supplier;
            this.accumulator = accumulator;
            this.combiner = combiner;
            this.finisher = finisher;
        }

        GafferCollectorImpl(final Supplier<A> supplier,
                            final BiConsumer<A, T> accumulator,
                            final BinaryOperator<A> combiner) {
            this(supplier, accumulator, combiner, i -> (R) i);
        }

        @Override
        public BiConsumer<A, T> accumulator() {
            return accumulator;
        }

        @Override
        public Supplier<A> supplier() {
            return supplier;
        }

        @Override
        public BinaryOperator<A> combiner() {
            return combiner;
        }

        @Override
        public Function<A, R> finisher() {
            return finisher;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return new HashSet<>();
        }
    }
}
