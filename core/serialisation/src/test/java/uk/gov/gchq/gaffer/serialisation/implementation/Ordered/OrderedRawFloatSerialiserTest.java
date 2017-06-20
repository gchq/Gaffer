package uk.gov.gchq.gaffer.serialisation.implementation.Ordered;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedRawFloatSerialiserTest {

    private static final OrderedRawFloatSerialiser SERIALISER = new OrderedRawFloatSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (float i = 0; i < 1000; i += 1.1) {
            byte[] b = SERIALISER.serialise(i);
            Object o = SERIALISER.deserialise(b);
            assertEquals(Float.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseFloatMinValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Float.MIN_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Float.class, o.getClass());
        assertEquals(Float.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseFloatMaxValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Float.MAX_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Float.class, o.getClass());
        assertEquals(Float.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseFloatClass() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(Float.class));
    }
}
