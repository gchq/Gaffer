/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.json;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HyperLogLogPlusJsonSerialisationTest {
    @Before
    public void before() {
        System.setProperty(JSONSerialiser.JSON_SERIALISER_MODULES, SketchesJsonModules.class.getName());
        JSONSerialiser.update();
    }

    @After
    public void after() {
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_MODULES);
        JSONSerialiser.update();
    }

    @Test
    public void testValidHyperLogLogPlusSketchSerialisedCorrectly() throws IOException {
        // Given
        final String testString = "TestString";

        final HyperLogLogPlus sketch = new HyperLogLogPlus(5, 5);
        sketch.offer(testString);

        // When / Then
        runTestWithSketch(sketch);
    }

    @Test
    public void testEmptyHyperLogLogPlusSketchIsSerialised() throws IOException {
        // Given
        final HyperLogLogPlus sketch = new HyperLogLogPlus(5, 5);

        // When / Then
        runTestWithSketch(sketch);
    }

    @Test
    public void testNullHyperLogLogPlusSketchIsSerialisedAsNullString() throws SerialisationException {
        // Given
        final HyperLogLogPlus sketch = null;

        // When
        final String sketchAsString = new String(JSONSerialiser.serialise(sketch));

        // Then - Serialisation framework will serialise nulls as 'null' string.
        assertEquals("null", sketchAsString);
    }

    @Test
    public void testNullHyperLogLogSketchDeserialisedAsEmptySketch() throws IOException {
        // Given
        final String sketchAsString = "{}";

        // When
        HyperLogLogPlus hllp = JSONSerialiser.deserialise(sketchAsString, HyperLogLogPlus.class);

        // Then
        assertEquals(0, hllp.cardinality());
    }

    @Test
    public void shouldDeserialiseNewHllpWithSAndSpValues() throws IOException {
        // Given
        final String sketchAsString = "{\"p\": 5, \"sp\": 10}";

        // When
        HyperLogLogPlus hllp = JSONSerialiser.deserialise(sketchAsString, HyperLogLogPlus.class);

        // Then
        assertArrayEquals(new HyperLogLogPlus(5, 10).getBytes(), hllp.getBytes());
    }

    @Test
    public void shouldDeserialiseNewNestedHllpWithSAndSpValues() throws IOException {
        // Given
        final String sketchAsString = "{\"hyperLogLogPlus\": {\"p\": 5, \"sp\": 10}}";

        // When
        HyperLogLogPlus hllp = JSONSerialiser.deserialise(sketchAsString, HyperLogLogPlus.class);

        // Then
        assertArrayEquals(new HyperLogLogPlus(5, 10).getBytes(), hllp.getBytes());
    }

    @Test
    public void shouldDeserialiseNewHllpWithOffers() throws IOException {
        // Given
        final String sketchAsString = "{\"p\": 5, \"sp\": 5, \"offers\": [\"value1\", \"value2\", \"value2\", \"value2\", \"value3\"]}";

        // When
        HyperLogLogPlus hllp = JSONSerialiser.deserialise(sketchAsString, HyperLogLogPlus.class);

        // Then
        HyperLogLogPlus expected = new HyperLogLogPlus(5, 5);
        expected.offer("value1");
        expected.offer("value2");
        expected.offer("value2");
        expected.offer("value2");
        expected.offer("value3");
        assertArrayEquals(expected.getBytes(), hllp.getBytes());
    }

    private void runTestWithSketch(final HyperLogLogPlus sketch) throws IOException {
        // When - serialise
        final String json = new String(JSONSerialiser.serialise(sketch));

        // Then - serialise
        JsonAssert.assertEquals("" +
                "{\"hyperLogLogPlus\":" +
                "{\"hyperLogLogPlusSketchBytes\":" + new String(JSONSerialiser.serialise(sketch.getBytes())) + "," +
                "\"cardinality\":" + sketch.cardinality() + "}}", json);

        // When - deserialise
        final HyperLogLogPlus deserialisedSketch = JSONSerialiser.deserialise(json, HyperLogLogPlus.class);

        // Then - deserialise
        assertNotNull(deserialisedSketch);
        assertEquals(sketch.cardinality(), deserialisedSketch.cardinality());
    }
}
