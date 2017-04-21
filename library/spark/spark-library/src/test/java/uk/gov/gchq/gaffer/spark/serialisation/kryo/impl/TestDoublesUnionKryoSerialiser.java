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
import com.yahoo.sketches.quantiles.DoublesUnion;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

public class TestDoublesUnionKryoSerialiser {
    private static final double DELTA = 0.01D;
    private final Kryo kryo = new Kryo();

    @Before
    public void setup() {
        new Registrator().registerClasses(kryo);
    }

    @Test
    public void testDoublesUnionKryoSerialiser() {
        final DoublesUnion union = DoublesUnion.builder().build();
        union.update(1.0D);
        union.update(2.0D);
        union.update(3.0D);
        test(union);

//        final DoublesUnion emptyUnion = DoublesUnion.builder().build();
//        test(emptyUnion);
    }

    private void test(final DoublesUnion doublesUnion) {
        final double quantile = doublesUnion.getResult().getQuantile(0.5D);

        // When
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        kryo.writeObject(output, doublesUnion);
        output.close();
        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final Input input = new Input(bais);
        final DoublesUnion read = kryo.readObject(input, DoublesUnion.class);
        input.close();

        // Then
        assertEquals(quantile, read.getResult().getQuantile(0.5D), DELTA);
    }
}
