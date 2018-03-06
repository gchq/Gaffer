/*
 * Copyright 2017-2018 Crown Copyright
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

import com.google.common.collect.Iterables;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.LimitedInMemorySortedIterable;

import java.util.LinkedHashSet;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors.toLimitedInMemorySortedIterable;
import static uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors.toLinkedHashSet;

public class GafferCollectorTest {

    @Test
    public void shouldCollectToLinkedHashSet() {
        // Given
        final IntStream stream = IntStream.range(0, 100);

        // When
        final Iterable<Integer> iterable = stream.boxed()
                .collect(toLinkedHashSet());

        // Then
        assertThat(iterable, instanceOf(LinkedHashSet.class));
        assertThat(Iterables.size(iterable), equalTo(100));
    }

    @Test
    public void shouldCollectToLimitedSortedSet() {
        // Given
        final IntStream stream = IntStream.range(0, 100);
        final int limit = 50;
        final boolean deduplicate = true;

        // When
        final LimitedInMemorySortedIterable<Integer> result = stream.boxed()
                .collect(toLimitedInMemorySortedIterable(Integer::compareTo, limit, deduplicate));

        // Then
        assertEquals(50, result.size());
    }
}
