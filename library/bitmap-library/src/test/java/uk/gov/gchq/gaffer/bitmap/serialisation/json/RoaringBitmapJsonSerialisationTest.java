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
package uk.gov.gchq.gaffer.bitmap.serialisation.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RoaringBitmapJsonSerialisationTest {

    private static final RoaringBitmapJsonSerialiser SERIALISER = new RoaringBitmapJsonSerialiser();
    private static final RoaringBitmapJsonDeserialiser DESERIALISER = new RoaringBitmapJsonDeserialiser();
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Test
    public void testCanSerialiseAndDeserialise() throws IOException {
        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(byteArrayBuilder);
        RoaringBitmap testBitmap = new RoaringBitmap();
        testBitmap.add(2);
        testBitmap.add(3000);
        testBitmap.add(300000);
        for (int i = 400000; i < 500000; i += 2) {
            testBitmap.add(i);
        }
        SERIALISER.serialize(testBitmap, jsonGenerator, null);
        jsonGenerator.flush();
        byte[] serialisedBitmap = byteArrayBuilder.toByteArray();
        JsonParser parser = JSON_FACTORY.createParser(serialisedBitmap);
        parser.setCodec(new ObjectMapper());
        Object o = DESERIALISER.deserialize(parser, null);
        assertEquals(RoaringBitmap.class, o.getClass());
        assertEquals(testBitmap, o);
    }

    @Test
    public void testCanSerialiseAndDeserialiseWithRuns() throws IOException {
        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(byteArrayBuilder);
        RoaringBitmap testBitmap = new RoaringBitmap();
        testBitmap.add(2);
        testBitmap.add(3000);
        testBitmap.add(300000);
        for (int i = 400000; i < 500000; i += 2) {
            testBitmap.add(i);
        }
        testBitmap.runOptimize();
        SERIALISER.serialize(testBitmap, jsonGenerator, null);
        jsonGenerator.flush();
        byte[] serialisedBitmap = byteArrayBuilder.toByteArray();
        JsonParser parser = JSON_FACTORY.createParser(serialisedBitmap);
        parser.setCodec(new ObjectMapper());
        Object o = DESERIALISER.deserialize(parser, null);
        assertEquals(RoaringBitmap.class, o.getClass());
        assertEquals(testBitmap, o);
    }

    @Test
    public void testCanDeserialiseVersionZeroPointThreePointFourBitmap() throws IOException {
        //Bitmap of (2,3000,300000) serialised in 0.3.4 Roaring Bitmap base 64 encoded
        String serialisedBitmap = "{\"roaringBitmap\":{\"value\":\"OTAAAAIAAAAAAAEABAAAAAIAuAvgkw==\"}}";
        RoaringBitmap comparisonBitmap = new RoaringBitmap();
        comparisonBitmap.add(2);
        comparisonBitmap.add(3000);
        comparisonBitmap.add(300000);
        JsonParser parser = JSON_FACTORY.createParser(serialisedBitmap);
        parser.setCodec(new ObjectMapper());
        Object o = DESERIALISER.deserialize(parser, null);
        assertEquals(RoaringBitmap.class, o.getClass());
        assertEquals(comparisonBitmap, o);
        ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(byteArrayBuilder);
        SERIALISER.serialize((RoaringBitmap) o, jsonGenerator, null);
        jsonGenerator.flush();
        byte[] bytes = byteArrayBuilder.toByteArray();
        String reSerialisedBitmap = new String(bytes);
        byteArrayBuilder = new ByteArrayBuilder();
        jsonGenerator = JSON_FACTORY.createGenerator(byteArrayBuilder);
        SERIALISER.serialize(comparisonBitmap, jsonGenerator, null);
        jsonGenerator.flush();
        String serialisedComparisonBitmap = new String(byteArrayBuilder.toByteArray());
        assertNotEquals(reSerialisedBitmap, serialisedBitmap);
        assertEquals(reSerialisedBitmap, serialisedComparisonBitmap);
    }

    @Test
    public void testCanSerialiseWithCustomObjectMapper() throws IOException {
        //Bitmap of (2,3000,300000) serialised in 0.5.11 Roaring Bitmap base 64 encoded
        String serialisedComparisonBitmap = "{\"roaringBitmap\":{\"value\":\"OjAAAAIAAAAAAAEABAAAABgAAAAcAAAAAgC4C+CT\"}}";
        RoaringBitmap comparisonBitmap = new RoaringBitmap();
        comparisonBitmap.add(2);
        comparisonBitmap.add(3000);
        comparisonBitmap.add(300000);
        final ObjectMapper mapper = JSONSerialiser.createDefaultMapper();
        final SimpleModule bitmapModule = new SimpleModule(RoaringBitmapConstants.BITMAP_MODULE_NAME, new Version(1, 0, 9, null, null, null));
        bitmapModule.addSerializer(RoaringBitmap.class, new RoaringBitmapJsonSerialiser());
        bitmapModule.addDeserializer(RoaringBitmap.class, new RoaringBitmapJsonDeserialiser());
        mapper.registerModule(bitmapModule);
        RoaringBitmap testBitmap = mapper.readValue(serialisedComparisonBitmap, RoaringBitmap.class);
        assertEquals(comparisonBitmap, testBitmap);
        String serialisedBitmap = mapper.writeValueAsString(testBitmap);
        assertEquals(serialisedBitmap, serialisedComparisonBitmap);
    }
}
