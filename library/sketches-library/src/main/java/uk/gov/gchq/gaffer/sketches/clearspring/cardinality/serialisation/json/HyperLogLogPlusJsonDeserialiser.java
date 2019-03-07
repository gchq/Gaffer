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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * A {@code HyperLogLogPlusJsonDeserialiser} deserialises {@link HyperLogLogPlus} objects.
 */
public class HyperLogLogPlusJsonDeserialiser extends JsonDeserializer<HyperLogLogPlus> {
    public static final int DEFAULT_P = 10;
    public static final int DEFAULT_SP = 10;

    @Override
    public HyperLogLogPlus deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        TreeNode treeNode = jsonParser.getCodec().readTree(jsonParser);
        final TreeNode nestedHllp = treeNode.get("hyperLogLogPlus");

        if (nonNull(nestedHllp)) {
            treeNode = nestedHllp;
        }

        final HyperLogLogPlus hllp;

        final TextNode jsonNodes = (TextNode) treeNode.get(HyperLogLogPlusJsonConstants.HYPER_LOG_LOG_PLUS_SKETCH_BYTES_FIELD);
        if (isNull(jsonNodes)) {
            final IntNode pNode = (IntNode) treeNode.get("p");
            final IntNode spNode = (IntNode) treeNode.get("sp");
            final int p = nonNull(pNode) ? pNode.asInt(DEFAULT_P) : DEFAULT_P;
            final int sp = nonNull(spNode) ? spNode.asInt(DEFAULT_SP) : DEFAULT_SP;
            hllp = new HyperLogLogPlus(p, sp);
        } else {
            hllp = HyperLogLogPlus.Builder.build(jsonNodes.binaryValue());
        }

        final ArrayNode offers = (ArrayNode) treeNode.get("offers");
        if (nonNull(offers)) {
            for (final JsonNode offer : offers) {
                if (nonNull(offer)) {
                    hllp.offer(offer.asText());
                }
            }
        }


        return hllp;
    }
}
