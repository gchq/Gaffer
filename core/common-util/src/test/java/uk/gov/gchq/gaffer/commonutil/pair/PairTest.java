/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.pair;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PairTest {

    @Test
    void shouldCreateMutablePair() {
        final Pair<Integer, String> pair = new Pair<>(0, "foo");

        assertThat(pair.getFirst().intValue()).isZero();
        assertThat(pair.getSecond()).isEqualTo("foo");
    }

    @Test
    void shouldCreateMutablePair2() {
        final Pair<Object, String> pair = new Pair<>(null, "bar");

        assertThat(pair.getFirst()).isNull();
        assertThat(pair.getSecond()).isEqualTo("bar");
    }

    @Test
    void shouldBeAbleToMutateFirstInPair() {
        final Pair<Integer, String> pair = new Pair<>(0);

        pair.setFirst(1);

        assertThat(pair.getFirst().intValue()).isEqualTo(1);
        assertThat(pair.getSecond()).isNull();
    }

    @Test
    void shouldBeAbleToMutateSecondInPair() {
        final Pair<Object, String> pair = new Pair<>();

        pair.setSecond("2nd");

        assertThat(pair.getFirst()).isNull();
        assertThat(pair.getSecond()).isEqualTo("2nd");
    }

    @Test
    void shouldReturnToString() {
        final Pair<String, String> pair = new Pair<>("foo", "bar");

        assertThat(pair).hasToString("Pair[first=foo,second=bar]");
    }
}
