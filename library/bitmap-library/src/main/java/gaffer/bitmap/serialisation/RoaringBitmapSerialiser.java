package gaffer.bitmap.serialisation;

import gaffer.bitmap.serialisation.utils.RoaringBitmapUtils;
import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RoaringBitmapSerialiser implements Serialisation {

    private static final long serialVersionUID = 3772387954385745791L;

    @Override
    public boolean canHandle(Class clazz) {
        return RoaringBitmap.class.equals(clazz);
    }

    @Override
    public byte[] serialise(Object object) throws SerialisationException {
        RoaringBitmap value = (RoaringBitmap) object;
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteOut);
        try {
            value.serialize(out);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return byteOut.toByteArray();
    }

    @Override
    public Object deserialise(byte[] bytes) throws SerialisationException {
        RoaringBitmap value = new RoaringBitmap();
        bytes = RoaringBitmapUtils.upConvertSerialisedForm(bytes);
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(byteIn);
        try {
            value.deserialize(in);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return value;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public Object deserialiseEmptyBytes() {
        return new RoaringBitmap();
    }

    @Override
    public byte[] serialiseNull() {
        return new byte[0];
    }

}
