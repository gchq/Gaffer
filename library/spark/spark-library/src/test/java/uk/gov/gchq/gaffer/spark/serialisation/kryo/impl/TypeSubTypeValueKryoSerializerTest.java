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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

public class TypeSubTypeValueKryoSerializerTest {
    private final Kryo kryo = new Kryo();

    @Before
    public void setup() { new Registrator().registerClasses(kryo); }

    @Test
    public void testTypeSubTypeValueKryoSerializer() {
        // Given
        final TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue("type", "subType", "value");

        // When
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final Output output = new Output(byteArrayOutputStream);
        kryo.writeObject(output, typeSubTypeValue);
        output.close();
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final Input input = new Input(byteArrayInputStream);
        final TypeSubTypeValue read = kryo.readObject(input, TypeSubTypeValue.class);
        input.close();

        // Then
        assertEquals(typeSubTypeValue, read);

    }
}
