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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * A {@code HyperLogLogPlusJsonDeserialiser} deserialises {@link HyperLogLogPlus}
 * objects. All objects including as {@link uk.gov.gchq.gaffer.types.TypeSubTypeValue}
 * are now properly supported. The only stipulation is that the {@code class}
 * must included in the fields of the {@code JSON} object. The object will not
 * be affected if it does not feature a {@code class} as this will be ignored
 * when the object is deserialised. NOTE: the {@code toString} method is
 * called by the {@link HyperLogLogPlus} so you need to ensure that this is
 * overridden by your object.
 */
public class HyperLogLogPlusJsonDeserialiser extends JsonDeserializer<HyperLogLogPlus> {
    public static final int DEFAULT_P = 10;
    public static final int DEFAULT_SP = 10;
    public static final String CLASS = "class";
    public static final String OFFERS = "offers";
    public static final String P = "p";
    public static final String SP = "sp";
    public static final String HYPER_LOG_LOG_PLUS = "hyperLogLogPlus";

    @Override
    public HyperLogLogPlus deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        TreeNode treeNode = jsonParser.getCodec().readTree(jsonParser);
        final TreeNode nestedHllp = treeNode.get(HYPER_LOG_LOG_PLUS);

        if (nonNull(nestedHllp)) {
            treeNode = nestedHllp;
        }

        final HyperLogLogPlus hllp;

        final TextNode jsonNodes = (TextNode) treeNode.get(HyperLogLogPlusJsonConstants.HYPER_LOG_LOG_PLUS_SKETCH_BYTES_FIELD);
        if (isNull(jsonNodes)) {
            final IntNode pNode = (IntNode) treeNode.get(P);
            final IntNode spNode = (IntNode) treeNode.get(SP);
            final int p = nonNull(pNode) ? pNode.asInt(DEFAULT_P) : DEFAULT_P;
            final int sp = nonNull(spNode) ? spNode.asInt(DEFAULT_SP) : DEFAULT_SP;
            hllp = new HyperLogLogPlus(p, sp);
        } else {
            hllp = HyperLogLogPlus.Builder.build(jsonNodes.binaryValue());
        }

        final ArrayNode offers = (ArrayNode) treeNode.get(OFFERS);

        if (nonNull(offers)) {
            for (final JsonNode offer : offers) {
                try {
                    final Object object = getOffer(offer);
                    if (object != null) {
                        hllp.offer(object);
                    }
                } catch (final Exception e) {
                    throw new SerialisationException("Error deserialising JSON object: " + e.getMessage(), e);
                }
            }
        }

        return hllp;
    }

    private static Object getOffer(final JsonNode offer) throws Exception {
        if (offer.hasNonNull(CLASS)) {
            try {
                return JSONSerialiser.deserialise(offer.toString(), Class.forName(offer.get(CLASS).asText()));
            } catch (final ClassNotFoundException e) {
                throw new IllegalArgumentException("Error converting the class [" + offer.get(CLASS).asText() + "] to a Java Object", e);
            }
        } else if (offer.fields().hasNext()) {
            // has fields so must be a map
            final Map<Object, Object> map = new HashMap<>();
            final Iterator<Map.Entry<String, JsonNode>> iterator = offer.fields();
            while (iterator.hasNext()) {
                final Map.Entry<String, JsonNode> entry = iterator.next();
                map.put(entry.getKey(), getOffer(entry.getValue()));
            }
            return map;
        } else if (offer.isArray()) {
            final List<Object> list = new ArrayList<>();
            final Iterator<JsonNode> iterator = offer.elements();
            while (iterator.hasNext()) {
                list.add(getOffer(iterator.next()));
            }
            return list;
        } else {
            return offer.asText();
        }
    }
}
