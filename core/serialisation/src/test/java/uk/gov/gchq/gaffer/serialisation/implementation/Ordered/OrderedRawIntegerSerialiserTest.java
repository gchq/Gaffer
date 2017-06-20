package uk.gov.gchq.gaffer.serialisation.implementation.Ordered;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedRawIntegerSerialiserTest {

    private static final OrderedRawIntegerSerialiser SERIALISER = new OrderedRawIntegerSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (int i = 0; i < 1000; i++) {
            byte[] b = SERIALISER.serialise(i);
            Object o = SERIALISER.deserialise(b);
            assertEquals(Integer.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseLongMinValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Integer.MIN_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(Integer.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseLongMaxValue() throws SerialisationException {
        byte[] b = SERIALISER.serialise(Integer.MAX_VALUE);
        Object o = SERIALISER.deserialise(b);
        assertEquals(Integer.class, o.getClass());
        assertEquals(Integer.MAX_VALUE, o);
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
