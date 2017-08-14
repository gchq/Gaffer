/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertNotNull;

public abstract class JSONSerialisationTest<T> {

    protected static final JSONSerialiser SERIALISER = new JSONSerialiser();

    @Test
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final T obj = getTestObject();

        // When
        final byte[] json = toJson(obj);
        final T deserialisedObj = fromJson(json);

        // Then
        assertNotNull(deserialisedObj);
    }

    protected abstract T getTestObject();

    protected byte[] toJson(final T testObj) {
        try {
            return SERIALISER.serialise(testObj, true);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    protected T fromJson(final byte[] jsonObj) {
        try {
            return SERIALISER.deserialise(jsonObj, (Class<T>) getTestObject().getClass());
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }
}
