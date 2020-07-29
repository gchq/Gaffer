/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.iterable;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RepeatItemIterableTest {

    @Test
    public void shouldRepeatItem5Times() {
        final String item = "item";
        final long repeats = 5;

        final Iterable<String> itr = new RepeatItemIterable<>(item, repeats);

        assertEquals(Lists.newArrayList(item, item, item, item, item), Lists.newArrayList(itr));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, -5})
    public void shouldRepeatItem0TimesWhenRepeatsIsEqualOrLessThanZero(long repeats) {
        final String item = "item";

        final Iterable<String> itr = new RepeatItemIterable<>(item, repeats);

        assertEquals(Lists.newArrayList(), Lists.newArrayList(itr));
    }
}
