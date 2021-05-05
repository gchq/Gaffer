/*
 * Copyright 2021 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ToTrailingWildcardPairTest extends FunctionTest {

    @Test
    public void shouldHandleNullInput() {
        // Given
        final ToTrailingWildcardPair function = new ToTrailingWildcardPair();

        // When
        final Pair<EntityId, EntityId> result = function.apply(null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldCreateEntityIdPair() {
        // Given
        final ToTrailingWildcardPair function = new ToTrailingWildcardPair();

        // When
        final Pair<EntityId, EntityId> result = function.apply("value1");

        // Then
        assertEquals("value1", result.getFirst().getVertex());
        assertEquals("value1~", result.getSecond().getVertex());
    }

    @Override
    protected ToTrailingWildcardPair getInstance() {
        return new ToTrailingWildcardPair();
    }

    @Override
    protected Class<? extends ToTrailingWildcardPair> getFunctionClass() {
        return ToTrailingWildcardPair.class;
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ToTrailingWildcardPair function = getInstance();
        function.setEndOfRange("*");

        // When
        final byte[] json = JSONSerialiser.serialise(function);
        final ToTrailingWildcardPair deserialisedObj = JSONSerialiser.deserialise(json, ToTrailingWildcardPair.class);

        // Then
        JsonAssert.assertEquals(
                "{\"class\":\"uk.gov.gchq.gaffer.operation.function.ToTrailingWildcardPair\", \"endOfRange\":\"*\"}",
                new String(json)
        );
        assertNotNull(deserialisedObj);
    }
}
