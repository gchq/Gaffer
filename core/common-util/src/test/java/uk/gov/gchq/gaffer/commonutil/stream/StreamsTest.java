/*
 * Copyright 2016-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class StreamsTest {

    @SuppressWarnings("unchecked")
    @Test
    void shouldCloseIteratorWhenStreamIsClosed() throws Throwable {
        // Given
        final Iterator<String> iterator = mock(Iterator.class, Mockito.withSettings().extraInterfaces(Closeable.class));
        given(iterator.hasNext()).willReturn(true, false);
        final String first = "first item";
        given(iterator.next()).willReturn(first, null, null);

        // When
        final Object firstResult;
        try (final Stream<?> stream = Streams.toStream(iterator)) {
            firstResult = stream.findFirst().orElseThrow(RuntimeException::new);
        }

        // Then
        assertThat(firstResult).isEqualTo(first);
        verify((Closeable) iterator).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCloseIterableWhenStreamIsClosed() throws Throwable {
        // Given
        final Iterable<String> iterable = mock(Iterable.class, Mockito.withSettings().extraInterfaces(Closeable.class));
        final Iterator<String> iterator = mock(Iterator.class, Mockito.withSettings().extraInterfaces(Closeable.class));

        given(iterable.spliterator()).willReturn(Spliterators.spliteratorUnknownSize(iterator, 0));
        given(iterator.hasNext()).willReturn(true, false);
        final String first = "first item";
        given(iterator.next()).willReturn(first, null, null);

        // When
        final Object firstResult;
        try (final Stream<?> stream = Streams.toStream(iterable)) {
            firstResult = stream.findFirst().orElseThrow(RuntimeException::new);
        }

        // Then
        assertThat(firstResult).isEqualTo("first item");
        verify((Closeable) iterable).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCloseIteratorWhenParallelStreamIsClosed() throws Throwable {
        // Given
        final Iterator<String> iterator = mock(Iterator.class, Mockito.withSettings().extraInterfaces(Closeable.class));
        given(iterator.hasNext()).willReturn(true, false);
        final String first = "first item";
        given(iterator.next()).willReturn(first, null, null);

        // When
        final Object firstResult;
        try (final Stream<?> stream = Streams.toParallelStream(iterator)) {
            firstResult = stream.findFirst().orElseThrow(RuntimeException::new);
        }

        // Then
        assertThat(firstResult).isEqualTo("first item");
        verify((Closeable) iterator).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldCloseIterableWhenParallelStreamIsClosed() throws Throwable {
        // Given
        final Iterable<String> iterable = mock(Iterable.class, Mockito.withSettings().extraInterfaces(Closeable.class));
        final Iterator<String> iterator = mock(Iterator.class, Mockito.withSettings().extraInterfaces(Closeable.class));
        given(iterable.spliterator()).willReturn(Spliterators.spliteratorUnknownSize(iterator, 0));
        given(iterator.hasNext()).willReturn(true, false);
        final String first = "first item";
        given(iterator.next()).willReturn(first, null, null);

        // When
        final Object firstResult;
        try (final Stream<?> stream = Streams.toParallelStream(iterable)) {
            firstResult = stream.findFirst().orElseThrow(RuntimeException::new);
        }

        // Then
        assertThat(firstResult).isEqualTo("first item");
        verify((Closeable) iterable).close();
    }
}
