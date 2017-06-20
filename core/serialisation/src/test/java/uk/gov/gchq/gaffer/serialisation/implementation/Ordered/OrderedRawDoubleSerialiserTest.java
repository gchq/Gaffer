package uk.gov.gchq.gaffer.serialisation.implementation.Ordered;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedRawDoubleSerialiserTest {

    private static final OrderedRawDoubleSerialiser SERIALISER = new OrderedRawDoubleSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (double i = 0; i < 1000; i++) {
            byte[] b = SERIALISER.serialise(i);
            Object o = SERIALISER.deserialise(b);
            assertEquals(Double.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseLongMinValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Double.MIN_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Double.class, o.getClass());
        assertEquals(Double.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseLongMaxValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Double.MAX_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Double.class, o.getClass());
        assertEquals(Double.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseDoubleClass() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(Double.class));
    }

}
