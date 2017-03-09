/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.serialisation.json.hyperloglogplus;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class HyperLogLogPlusJsonSerialisationTest {
    private ObjectMapper mapper;

    @Before
    public void before() {
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);

        final SimpleModule module = new SimpleModule(HyperLogLogPlusJsonConstants.HYPER_LOG_LOG_PLUS_SERIALISER_MODULE_NAME, new Version(1, 0, 0, null));
        module.addSerializer(HyperLogLogPlus.class, new HyperLogLogPlusJsonSerialiser());
        module.addDeserializer(HyperLogLogPlus.class, new HyperLogLogPlusJsonDeserialiser());

        mapper.registerModule(module);
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
    public void testNullHyperLogLogPlusSketchIsSerialisedAsNullString() throws JsonProcessingException {
        // Given
        final HyperLogLogPlus sketch = null;

        // When
        final String sketchAsString = mapper.writeValueAsString(sketch);

        // Then - Serialisation framework will serialise nulls as 'null' string.
        assertEquals("null", sketchAsString);
    }

    public void testNullHyperLogLogSketchDeserialisedAsEmptySketch() throws IOException {
        // Given
        final String sketchAsString = "{}";

        // When / Then
        try {
            // TODO - See 'Can't easily create HyperLogLogPlus sketches in JSON'
            mapper.readValue(IOUtils.toInputStream(sketchAsString), HyperLogLogPlus.class);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }
    }

    private void runTestWithSketch(final HyperLogLogPlus sketch) throws IOException {
        // When - serialise
        final String json = mapper.writeValueAsString(sketch);

        // Then - serialise
        final String[] parts = json.split("[:,]");
        final String[] expectedParts = {
                "{\"hyperLogLogPlus\"",
                "{\"hyperLogLogPlusSketchBytes\"",
                "BYTES",
                "\"cardinality\"",
                sketch.cardinality() + "}}"};
        for (int i = 0; i < parts.length; i++) {
            if (2 != i) { // skip checking the bytes
                assertEquals(expectedParts[i], parts[i]);
            }
        }

        // When - deserialise
        final HyperLogLogPlus deserialisedSketch = mapper.readValue(IOUtils.toInputStream(json), HyperLogLogPlus.class);

        // Then - deserialise
        assertNotNull(deserialisedSketch);
        assertEquals(sketch.cardinality(), deserialisedSketch.cardinality());
    }
}
