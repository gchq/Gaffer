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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;

public class HyperLogLogPlusJsonDeserialiser extends JsonDeserializer<HyperLogLogPlus> {

    // TODO - See 'Can't create HyperLogLogPlus sketches in JSON'
    @Override
    public HyperLogLogPlus deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

        final TreeNode treeNode = jsonParser.getCodec().readTree(jsonParser);

        final TreeNode coreHyperLogLogPlusObject = treeNode.get("hyperLogLogPlus");
        if (coreHyperLogLogPlusObject != null) {
            final TextNode jsonNodes = (TextNode) coreHyperLogLogPlusObject.get(HyperLogLogPlusJsonConstants.HYPER_LOG_LOG_PLUS_SKETCH_BYTES_FIELD);

            final byte[] nodeAsString = jsonNodes.binaryValue();
            final HyperLogLogPlus hyperLogLogPlus = HyperLogLogPlus.Builder.build(nodeAsString);

            return hyperLogLogPlus;
        } else {
            throw new IllegalArgumentException("Recieved null or empty HyperLogLogPlus sketch");
        }
    }
}
