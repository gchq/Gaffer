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
package uk.gov.gchq.gaffer.bitmap.serialisation.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import org.roaringbitmap.RoaringBitmap;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RoaringBitmapJsonSerialiser extends JsonSerializer<RoaringBitmap> {

    @Override
    public void serialize(final RoaringBitmap roaringBitmap, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        _serialise(roaringBitmap, jsonGenerator);
        jsonGenerator.writeEndObject();
    }

    @Override
    public void serializeWithType(final RoaringBitmap value, final JsonGenerator gen, final SerializerProvider serializers, final TypeSerializer typeSer) throws IOException {
        typeSer.writeTypePrefixForObject(value, gen);
        _serialise(value, gen);
        typeSer.writeTypeSuffixForObject(value, gen);
    }

    private void _serialise(final RoaringBitmap roaringBitmap, final JsonGenerator jsonGenerator) throws  IOException {
        jsonGenerator.writeObjectFieldStart(RoaringBitmapConstants.BITMAP_WRAPPER_OBJECT_NAME);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        roaringBitmap.serialize(new DataOutputStream(baos));
        jsonGenerator.writeObjectField(RoaringBitmapConstants.BITMAP_VALUE_FIELD_NAME, baos.toByteArray());
        baos.close();
        jsonGenerator.writeEndObject();
    }

}
