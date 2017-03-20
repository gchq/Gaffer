/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.function.filter;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.predicate.PredicateTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IsShorterThanTest extends PredicateTest {
    @Test
    public void shouldSetAndGetMaxLength() {
        // Given
        final IsShorterThan filter = new IsShorterThan(5);

        // When
        final int maxLength1 = filter.getMaxLength();
        filter.setMaxLength(10);
        final int maxLength2 = filter.getMaxLength();

        // Then
        assertEquals(5, maxLength1);
        assertEquals(10, maxLength2);
    }

    @Test
    public void shouldAcceptTheValueWhenLessThan() {
        // Given
        final IsShorterThan filter = new IsShorterThan(5);

        // When
        final boolean accepted = filter.test("1234");

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenMoreThan() {
        // Given
        final IsShorterThan filter = new IsShorterThan(5);

        // When
        final boolean accepted = filter.test("123456");

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenEqual() {
        // Given
        final IsShorterThan filter = new IsShorterThan(5);

        // When
        final boolean accepted = filter.test("12345");

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldThrowExceptionWhenTheValueWhenUnknownType() {
        // Given
        final IsShorterThan filter = new IsShorterThan(5);

        // When / Then
        try {
            filter.test(4);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final int max = 5;
        final IsShorterThan filter = new IsShorterThan(max);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.IsShorterThan\",%n" +
                "  \"orEqualTo\" : false,%n" +
                "  \"maxLength\" : 5%n" +
                "}"), json);

        // When 2
        final IsShorterThan deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), IsShorterThan.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(max, deserialisedFilter.getMaxLength());
    }

    @Override
    protected Class<IsShorterThan> getPredicateClass() {
        return IsShorterThan.class;
    }

    @Override
    protected IsShorterThan getInstance() {
        return new IsShorterThan(5);
    }
}
