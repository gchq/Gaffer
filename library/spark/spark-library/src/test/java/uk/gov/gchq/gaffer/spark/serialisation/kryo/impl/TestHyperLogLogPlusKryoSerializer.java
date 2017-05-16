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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class TestHyperLogLogPlusKryoSerializer {
    private final Kryo kryo = new Kryo();

    @Before
    public void setup() {
        new Registrator().registerClasses(kryo);
    }

    @Test
    public void testHyperLogLogPlusKryoSerialiser() {
        // HyperLogLogPlus with p = 5, sp = 5
        final HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(5, 5);
        IntStream.range(0, 1000).forEach(i -> hyperLogLogPlus.offer("" + i));
        test(hyperLogLogPlus);

        // HyperLogLogPlus with p = 15
        final HyperLogLogPlus hyperLogLogPlus2 = new HyperLogLogPlus(15);
        IntStream.range(0, 10000).forEach(i -> hyperLogLogPlus2.offer("" + i));
        test(hyperLogLogPlus2);
    }

    private void test(final HyperLogLogPlus hyperLogLogPlus) {
        // When
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Output output = new Output(baos);
        kryo.writeObject(output, hyperLogLogPlus);
        output.close();
        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final Input input = new Input(bais);
        final HyperLogLogPlus read = kryo.readObject(input, HyperLogLogPlus.class);
        input.close();

        // Then
        assertEquals(hyperLogLogPlus.cardinality(), read.cardinality());
    }
}
