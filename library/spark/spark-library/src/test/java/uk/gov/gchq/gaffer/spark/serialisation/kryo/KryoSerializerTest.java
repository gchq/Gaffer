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
package uk.gov.gchq.gaffer.spark.serialisation.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

/**
 * Abstract test class provided for convenience.
 * @param <T> the class for which the serialiser is written
 */
public abstract class KryoSerializerTest<T> {
    private final Kryo kryo = new Kryo();

    @Before
    public void setup() { new Registrator().registerClasses(kryo);}

    @Test
    public void shouldSerialiseAndDeserialise() {
        // Given
        final T obj = getTestObject();

        // When
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final Output output = new Output(byteArrayOutputStream);
        kryo.writeObject(output, obj);
        output.close();
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final Input input = new Input(byteArrayInputStream);
        final T read = kryo.readObject(input, getTestClass());
        input.close();

        // Then
        assertEquals(obj, read);
    }

    /**
     * Due to type erasure, this is required for deserialisation.
     * @return the class of type T, for which the serialiser is written
     */
    protected abstract Class<T> getTestClass();

    /**
     * Should return a new object of the class T under test, constructed as necessary.
     * @return a new object of type T, to be serialised/deserialised
     */
    protected abstract T getTestObject();
}
