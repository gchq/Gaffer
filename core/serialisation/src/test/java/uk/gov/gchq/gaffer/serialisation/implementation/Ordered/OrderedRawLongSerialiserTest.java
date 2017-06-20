package uk.gov.gchq.gaffer.serialisation.implementation.Ordered;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedRawLongSerialiserTest {

    private static final OrderedRawLongSerialiser SERIALISER = new OrderedRawLongSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (long i = 0; i < 1000; i++) {
            byte[] b = SERIALISER.serialise(i);
            Object o = SERIALISER.deserialise(b);
            assertEquals(Long.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseLongMinValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Long.MIN_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Long.class, o.getClass());
        assertEquals(Long.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseLongMaxValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Long.MAX_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Long.class, o.getClass());
        assertEquals(Long.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseLongClass() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(Long.class));
    }
}
