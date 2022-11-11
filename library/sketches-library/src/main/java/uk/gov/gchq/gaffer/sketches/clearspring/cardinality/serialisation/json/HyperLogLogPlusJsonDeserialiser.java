/*
 * Copyright 2016-2022 Crown Copyright
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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.IOException;

import static java.util.Objects.nonNull;

/**
 * A {@code HyperLogLogPlusJsonDeserialiser} deserialises {@link HyperLogLogPlus}
 * objects. All objects including {@link uk.gov.gchq.gaffer.types.TypeValue},
 * {@link uk.gov.gchq.gaffer.types.TypeSubTypeValue},
 * {@link uk.gov.gchq.gaffer.types.CustomMap} and
 * {@link uk.gov.gchq.gaffer.types.FreqMap} are now properly supported. The only
 * stipulation is that the {@code class} must included in the fields of the
 * {@code JSON} object. This can be done in {@code Java} by including the
 * {@code class} {@code annotation}:
 * <pre>
 * <code>&#064;JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")</code>
 * </pre>
 * The object will not be affected if it does not feature a {@code class} as
 * this will be ignored when the object is deserialised. Unfortunately,
 * you cannot have arrays of values as {@code offers} is not permitted.
 * <p>
 * <b>
 * NOTE: the {@code toString} method is called by the {@link HyperLogLogPlus}
 * class when making the {@code offers} so you need to ensure that the
 * {@code toString} method is overridden by your object.
 * </b>
 * </p>
 */
public class HyperLogLogPlusJsonDeserialiser extends JsonDeserializer<HyperLogLogPlus> {

    public static final String CLASS = "class";

    public static final String HYPER_LOG_LOG_PLUS = "hyperLogLogPlus";

    @Override
    public HyperLogLogPlus deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        try {
            TreeNode treeNode = jsonParser.getCodec().readTree(jsonParser);
            final TreeNode nestedHllp = treeNode.get(HYPER_LOG_LOG_PLUS);

            if (nonNull(nestedHllp)) {
                treeNode = nestedHllp;
            }

            final HyperLogLogPlusWithOffers hyperLogLogPlusWithOffers = JSONSerialiser.deserialise(treeNode.toString(), HyperLogLogPlusWithOffers.class);
            return hyperLogLogPlusWithOffers.getHyperLogLogPlus();
        } catch (final Exception e) {
            throw new SerialisationException("Error deserialising JSON object: " + e.getMessage(), e);
        }
    }
}
