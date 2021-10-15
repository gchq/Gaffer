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

package uk.gov.gchq.gaffer.commonutil.iterable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

public class RepeatItemIterableTest {

    @Test
    public void shouldRepeatItem5Times() {
        final String item = "item";
        final long repeats = 5;

        final Iterable<String> itr = new RepeatItemIterable<>(item, repeats);

        assertThat(itr)
                .hasSize(5)
                .allMatch(str -> str.equals(item));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, -5})
    public void shouldRepeatItem0TimesWhenRepeatsIsEqualOrLessThanZero(long repeats) {
        final String item = "item";

        final Iterable<String> itr = new RepeatItemIterable<>(item, repeats);

        assertThat(itr).isEmpty();
    }
}
