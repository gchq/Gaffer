package gaffer.bitmap.serialisation.json;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.TextNode;
import gaffer.bitmap.serialisation.RoaringBitmapSerialiser;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public class RoaringBitmapJsonDeserialiser extends JsonDeserializer<RoaringBitmap> {

    private final RoaringBitmapSerialiser bitmapSerialiser = new RoaringBitmapSerialiser();

    @Override
    public RoaringBitmap deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        final TreeNode treeNode = jsonParser.getCodec().readTree(jsonParser);
        final TreeNode bitmapObject = treeNode.get(RoaringBitmapConstants.BITMAP_WRAPPER_OBJECT_NAME);
        if (bitmapObject != null) {
            final TextNode jsonNodes = (TextNode) bitmapObject.get(RoaringBitmapConstants.BITMAP_VALUE_FIELD_NAME);
            return (RoaringBitmap)bitmapSerialiser.deserialise(jsonNodes.binaryValue());
        } else {
            throw new IllegalArgumentException("Received null bitmap treenode");
        }
    }

}
