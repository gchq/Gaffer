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


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.TextNode;
import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.bitmap.serialisation.RoaringBitmapSerialiser;
import java.io.IOException;

public class RoaringBitmapJsonDeserialiser extends JsonDeserializer<RoaringBitmap> {

    private final RoaringBitmapSerialiser bitmapSerialiser = new RoaringBitmapSerialiser();

    @Override
    public RoaringBitmap deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        final TreeNode treeNode = jsonParser.getCodec().readTree(jsonParser);
        final TreeNode bitmapObject = treeNode.get(RoaringBitmapConstants.BITMAP_WRAPPER_OBJECT_NAME);
        if (bitmapObject != null) {
            final TextNode jsonNodes = (TextNode) bitmapObject.get(RoaringBitmapConstants.BITMAP_VALUE_FIELD_NAME);
            return (RoaringBitmap) bitmapSerialiser.deserialise(jsonNodes.binaryValue());
        } else {
            throw new IllegalArgumentException("Received null bitmap treenode");
        }
    }

}
