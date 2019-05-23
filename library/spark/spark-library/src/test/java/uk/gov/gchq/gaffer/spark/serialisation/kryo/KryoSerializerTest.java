/*
 * Copyright 2017-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.spark.serialisation.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Abstract test class provided for convenience.
 *
 * @param <T> the class for which the serialiser is written
 */
public abstract class KryoSerializerTest<T> {
    private Kryo kryo;

    @Before
    public void setup() {
        kryo = new Kryo();
        new Registrator().registerClasses(kryo);
    }

    @Test
    public void shouldSerialiseAndDeserialise() {
        // Given
        final T obj = getTestObject();
        final Class<T> testClass = getTestClass();

        // When
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (Output output = new Output(byteArrayOutputStream)) {
            kryo.writeObject(output, obj);
        }

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final T read;
        try (Input input = new Input(byteArrayInputStream)) {
            read = kryo.readObject(input, testClass);
        }

        // Then
        shouldCompareSerialisedAndDeserialisedObjects(obj, read);
    }

    /**
     * This method should compare a test object against a deserialised object of the same type,
     * in a manner suitable for the object.
     *
     * @param obj          the serialised obj
     * @param deserialised the deserialised obj
     */
    protected abstract void shouldCompareSerialisedAndDeserialisedObjects(final T obj, final T deserialised);

    /**
     * Due to type erasure, this is required for deserialisation.
     *
     * @return the class of type T, for which the serialiser is written
     */
    protected abstract Class<T> getTestClass();

    /**
     * Should return a new object of the class T under test, constructed as necessary.
     *
     * @return a new object of type T, to be serialised/deserialised
     */
    protected abstract T getTestObject();
}
