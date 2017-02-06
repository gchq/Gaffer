package gaffer.bitmap.serialisation;

import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;

public class RoaringBitmapSerialiserTest {

    private static final RoaringBitmapSerialiser SERIALISER = new RoaringBitmapSerialiser();

    @Test
    public void testCanSerialiseAndDeserialise() throws SerialisationException {
        RoaringBitmap testBitmap = new RoaringBitmap();
        testBitmap.add(2);
        testBitmap.add(3000);
        testBitmap.add(300000);

        for (int i=400000; i<500000; i+=2) {
            testBitmap.add(i);
        }

        byte[] b = SERIALISER.serialise(testBitmap);
        Object o = SERIALISER.deserialise(b);
        assertEquals(RoaringBitmap.class, o.getClass());
        assertEquals(testBitmap, o);
    }

}
