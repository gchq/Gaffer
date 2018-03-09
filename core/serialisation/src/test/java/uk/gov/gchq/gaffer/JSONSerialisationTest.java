/*
 * Copyright 2017-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * Provides a common interface for testing classes that should be JSON serialisable.
 *
 * @param <T> Object of type T that is to be tested
 */
public abstract class JSONSerialisationTest<T> {
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

    @Test
    public void shouldHaveJsonPropertyAnnotation() throws Exception {
        // Given
        final T op = getTestObject();

        // When
        final JsonPropertyOrder annotation = op.getClass().getAnnotation(JsonPropertyOrder.class);

        // Then
        assumeTrue("Missing JsonPropertyOrder annotation on class. It should de defined and set to alphabetical." + op.getClass().getName(),
                null != annotation && annotation.alphabetic());
    }

    /**
     * This method should be used to generate an instance of the object under test,
     * eg. return new foo();
     * The object can be no arg, or as complex as is necessary for the test(s),
     * eg. return new bar(arg1, arg2, ...);
     *
     * @return an instance of the object under test of type T
     */
    protected abstract T getTestObject();

    protected byte[] toJson(final T testObj) {
        try {
            return JSONSerialiser.serialise(testObj, true);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    protected T fromJson(final byte[] jsonObj) {
        try {
            return JSONSerialiser.deserialise(jsonObj, (Class<T>) getTestObject().getClass());
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }
}
