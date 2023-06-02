/*
 * Copyright 2016-2023 Crown Copyright
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.datasketches.hll.HllSketch;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

/**
 * A {@code HllSketchJsonDeserialiser} deserialises {@link HllSketch} objects.
 */
public class HllSketchJsonDeserialiser extends JsonDeserializer<HllSketch> {

    @Override
    public HllSketch deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws SerialisationException {
        try {
            final TreeNode treeNode = jsonParser.getCodec().readTree(jsonParser);
            final HllSketchWithValues hllSketchWithValues = JSONSerialiser.deserialise(treeNode.toString(), HllSketchWithValues.class);
            return hllSketchWithValues.getHllSketch();
        } catch (final Exception e) {
            throw new SerialisationException("Error deserialising JSON object: " + e.getMessage(), e);
        }
    }
}
