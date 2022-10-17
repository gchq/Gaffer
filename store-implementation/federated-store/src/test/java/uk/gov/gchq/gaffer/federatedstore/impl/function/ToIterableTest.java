/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.impl.function;


import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.EmptyIterable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ToIterableTest {


    public static final ToIterable TO_ITERABLE = new ToIterable();

    @Test
    public void shouldReturnEmptyIterableFromNull() {
        assertThat(TO_ITERABLE.apply(null))
                .isInstanceOf(EmptyIterable.class);
    }

    @Test
    public void shouldReturnSameIterableFromList() {
        final List<Integer> ints = Arrays.asList(1, 2, 3, 4);

        assertThat(TO_ITERABLE.apply(ints))
                .containsExactlyInAnyOrder(1, 2, 3, 4)
                .isSameAs(ints);
    }

    @Test
    public void shouldReturnSameIterableFromCollection() {
        final List<Integer> ints = Collections.unmodifiableList(Arrays.asList(1, 2, 3, 4));

        assertThat(TO_ITERABLE.apply(ints))
                .containsExactlyInAnyOrder(1, 2, 3, 4)
                .isSameAs(ints);
    }

    @Test
    public void shouldReturnIterableFromArray() {
        final int[] ints = {1, 2, 3, 4};

        assertThat(TO_ITERABLE.apply(ints))
                .asInstanceOf(InstanceOfAssertFactories.iterable(Integer.class))
                .containsExactlyInAnyOrder(1, 2, 3, 4);
    }

    @Test
    public void shouldShouldReturnIterableFromANonPluralObject() {
        assertThat(TO_ITERABLE.apply(1))
                .asInstanceOf(InstanceOfAssertFactories.iterable(Integer.class))
                .containsExactly(1);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        final ToIterable toIterable = new ToIterable();

        final String json = String.format("{%n  \"class\" : \"uk.gov.gchq.gaffer.federatedstore.impl.function.ToIterable\"%n}");

        final String serialised = new String(JSONSerialiser.serialise(toIterable, true));

        final ToIterable deserialise = JSONSerialiser.deserialise(json, ToIterable.class);

        assertThat(serialised).isEqualTo(json);
        assertThat(deserialise).isEqualTo(toIterable);
    }
}
