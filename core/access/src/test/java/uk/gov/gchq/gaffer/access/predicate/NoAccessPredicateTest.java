/*
 * Copyright 2020-2021 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class NoAccessPredicateTest implements AccessPredicateTest {
    @Test
    public void shouldAlwaysReturnFalse() {
        assertFalse(new NoAccessPredicate().test(null, null));
        assertFalse(new NoAccessPredicate().test(new User.Builder().build(), ""));
        assertFalse(new NoAccessPredicate().test(new User.Builder().userId("someone").build(), "anything"));
    }

    @Test
    @Override
    public void canBeJsonSerialisedAndDeserialised() throws Exception {
        final AccessPredicate predicate = new NoAccessPredicate();
        final byte[] bytes = JSONSerialiser.serialise(predicate);
        assertEquals("{" +
                "\"class\":\"uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate\"" +
                "}", new String(bytes, CommonConstants.UTF_8));
        assertEquals(predicate, JSONSerialiser.deserialise(bytes, NoAccessPredicate.class));
    }

    @Test
    @Override
    public void shouldReturnTrueForEqualObjectComparisonWhenEqual() {
        assertEquals(new NoAccessPredicate(), new NoAccessPredicate());
    }

    @Test
    @Override
    public void shouldReturnFalseForEqualObjectComparisonWhenNotEqual() {
        /* not possible */
    }
}
