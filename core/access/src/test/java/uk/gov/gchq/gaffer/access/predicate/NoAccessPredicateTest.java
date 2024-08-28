/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.access.predicate;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class NoAccessPredicateTest {

    @Test
    void shouldAlwaysReturnFalse() {
        assertThat(new NoAccessPredicate().test(null, null)).isFalse();
        assertThat(new NoAccessPredicate().test(new User.Builder().build(), "")).isFalse();
        assertThat(new NoAccessPredicate().test(new User.Builder().userId("someone").build(), "anything")).isFalse();
    }

    @Test
    void canBeJsonSerialisedAndDeserialised() throws Exception {
        final AccessPredicate predicate = new NoAccessPredicate();
        final byte[] bytes = JSONSerialiser.serialise(predicate);
        final String expectedString = "{\"class\":\"uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate\"}";
        assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo(expectedString);
        assertThat(JSONSerialiser.deserialise(bytes, NoAccessPredicate.class)).isEqualTo(predicate);
    }

    @Test
    void shouldReturnTrueForEqualObjectComparisonWhenEqual() {
        assertThat(new NoAccessPredicate()).isEqualTo(new NoAccessPredicate());
    }
}
