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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

import java.io.IOException;

/**
 * A {@code HyperLogLogPlusJsonSerialiser} serialises {@link HyperLogLogPlus} objects.
 */
public class HyperLogLogPlusJsonSerialiser extends JsonSerializer<HyperLogLogPlus> {

    @Override
    public void serialize(final HyperLogLogPlus hyperLogLogPlus, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();
        _serialise(hyperLogLogPlus, jsonGenerator);
        jsonGenerator.writeEndObject();
    }

    @Override
    public void serializeWithType(final HyperLogLogPlus value, final JsonGenerator gen, final SerializerProvider serializers, final TypeSerializer typeSer) throws IOException {
        typeSer.writeTypePrefixForObject(value, gen);
        _serialise(value, gen);
        typeSer.writeTypeSuffixForObject(value, gen);
    }

    @Override
    public Class<HyperLogLogPlus> handledType() {
        return HyperLogLogPlus.class;
    }

    private void _serialise(final HyperLogLogPlus hyperLogLogPlus, final JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeObjectFieldStart("hyperLogLogPlus");
        jsonGenerator.writeObjectField(HyperLogLogPlusJsonConstants.HYPER_LOG_LOG_PLUS_SKETCH_BYTES_FIELD, hyperLogLogPlus.getBytes());
        jsonGenerator.writeNumberField(HyperLogLogPlusJsonConstants.CARDINALITY_FIELD, hyperLogLogPlus.cardinality());
        jsonGenerator.writeEndObject();
    }
}
