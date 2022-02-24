/*
 * Copyright 2017-2021 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.LimitedInMemorySortedIterable;

import java.util.LinkedHashSet;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors.toLimitedInMemorySortedIterable;
import static uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors.toLinkedHashSet;

public class GafferCollectorTest {

    @Test
    public void shouldCollectToLinkedHashSet() {
        final IntStream stream = IntStream.range(0, 100);

        final Iterable<Integer> iterable = stream.boxed()
                .collect(toLinkedHashSet());

        assertThat(iterable)
                .isInstanceOf(LinkedHashSet.class)
                .hasSize(100);
    }

    @Test
    public void shouldCollectToLimitedSortedSet() {
        final IntStream stream = IntStream.range(0, 100);
        final int limit = 50;
        final boolean deduplicate = true;

        final LimitedInMemorySortedIterable<Integer> result = stream.boxed()
                .collect(toLimitedInMemorySortedIterable(Integer::compareTo, limit, deduplicate));

        assertThat(result).hasSize(50);
    }
}
