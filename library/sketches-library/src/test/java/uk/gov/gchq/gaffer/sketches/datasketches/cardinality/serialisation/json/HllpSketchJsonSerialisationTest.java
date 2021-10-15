/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.serialisation.json;

import com.yahoo.sketches.hll.HllSketch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class HllpSketchJsonSerialisationTest {

    @BeforeEach
    public void before() {
        System.setProperty(JSONSerialiser.JSON_SERIALISER_MODULES, SketchesJsonModules.class.getName());
        JSONSerialiser.update();
    }

    @AfterEach
    public void after() {
        System.clearProperty(JSONSerialiser.JSON_SERIALISER_MODULES);
        JSONSerialiser.update();
    }

    @Test
    public void testValidHyperLogLogPlusSketchSerialisedCorrectly() throws IOException {
        // Given
        final String testString = "TestString";

        final HllSketch sketch = new HllSketch(10);
        sketch.update(testString);

        // When / Then
        runTestWithSketch(sketch);
    }

    @Test
    public void testEmptyHyperLogLogPlusSketchIsSerialised() throws IOException {
        // Given
        final HllSketch sketch = new HllSketch(10);

        // When / Then
        runTestWithSketch(sketch);
    }

    @Test
    public void testNullHyperLogLogPlusSketchIsSerialisedAsNullString() throws SerialisationException {
        // Given
        final HllSketch sketch = null;

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
        HllSketch hllp = JSONSerialiser.deserialise(sketchAsString, HllSketch.class);

        // Then
        assertEquals(0, hllp.getEstimate(), 0.001);
    }

    @Test
    public void shouldDeserialiseNewHllpWithSAndSpValues() throws IOException {
        // Given
        final String sketchAsString = "{\"logK\": 20}";

        // When
        HllSketch hllp = JSONSerialiser.deserialise(sketchAsString, HllSketch.class);

        // Then
        assertArrayEquals(new HllSketch(20).toCompactByteArray(), hllp.toCompactByteArray());
    }

    @Test
    public void shouldDeserialiseNewHllpWithValues() throws IOException {
        // Given
        final String sketchAsString = "{\"logK\": 20, \"values\": [\"value1\", \"value2\", \"value2\", \"value2\", \"value3\"]}";

        // When
        HllSketch hllp = JSONSerialiser.deserialise(sketchAsString, HllSketch.class);

        // Then
        HllSketch expected = new HllSketch(20);
        expected.update("value1");
        expected.update("value2");
        expected.update("value2");
        expected.update("value2");
        expected.update("value3");
        assertArrayEquals(expected.toCompactByteArray(), hllp.toCompactByteArray());
    }

    private void runTestWithSketch(final HllSketch sketch) throws IOException {
        // When - serialise
        final String json = new String(JSONSerialiser.serialise(sketch));

        // Then - serialise
        JsonAssert.assertEquals("" +
                "{\"bytes\":" + new String(JSONSerialiser.serialise(sketch.toCompactByteArray())) + "," +
                "\"cardinality\":" + sketch.getCompositeEstimate() + "}", json);

        // When - deserialise
        final HllSketch deserialisedSketch = JSONSerialiser.deserialise(json, HllSketch.class);

        // Then - deserialise
        assertNotNull(deserialisedSketch);
        assertEquals(sketch.getCompositeEstimate(), deserialisedSketch.getEstimate(), 0.0001);
        assertArrayEquals(sketch.toCompactByteArray(), deserialisedSketch.toCompactByteArray());
    }
}
