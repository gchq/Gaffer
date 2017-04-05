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

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility class to help with the usage of the Java 8 Streams API in Gaffer.
 */
public final class Streams {

    /**
     * Convert an {@link java.lang.Iterable} to a {@link java.util.stream.Stream}
     *
     * @param iterable the input iterable
     * @param <T> the type of object stored in the iterable
     * @return a stream containing the contents of the iterable
     */
    public static <T> Stream<T> toStream(final Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Convert an {@link java.util.Iterator} to a {@link java.util.stream.Stream}
     *
     * @param iterator the input iterator
     * @param <T> the type of object stored in the iterator
     * @return a stream containing the contents of the iterator
     */
    public static <T> Stream<T> toStream(final Iterator<T> iterator) {
        return toStream(() -> iterator);
    }

    /**
     * Convert an {@link java.lang.Iterable} to a {@link java.util.stream.Stream}
     *
     * @param iterable the input iterable
     * @param <T> the type of object stored in the iterable
     * @return a stream containing the contents of the iterable
     */
    public static <T> Stream<T> toParallelStream(final Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), true);
    }

    /**
     * Convert an {@link java.util.Iterator} to a {@link java.util.stream.Stream}
     *
     * @param iterator the input iterator
     * @param <T> the type of object stored in the iterator
     * @return a stream containing the contents of the iterator
     */
    public static <T> Stream<T> toParallelStream(final Iterator<T> iterator) {
        return toParallelStream(() -> iterator);
    }

    private Streams() {
        // Empty
    }
}
