package uk.gov.gchq.gaffer.serialisation.implementation.Ordered;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedRawDateSerialiserTest {

    private static final OrderedRawDateSerialiser SERIALISER = new OrderedRawDateSerialiser();

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (long i = 1000000L; i < 1001000L; i++) {
            final byte[] b = SERIALISER.serialise(new Date(i));
            final Object o = SERIALISER.deserialise(b);
            assertEquals(Date.class, o.getClass());
            assertEquals(new Date(i), o);
        }
    }

    @Test
    public void canSerialiseEpoch() throws SerialisationException {
        final byte[] b = SERIALISER.serialise(new Date(0));
        final Object o = SERIALISER.deserialise(b);
        assertEquals(Date.class, o.getClass());
        assertEquals(new Date(0), o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void canSerialiseDateClass() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(Date.class));
    }

    @Test
    public void checkOrderPreserved() throws SerialisationException {
        Date now = new Date(1L);
        Date aDayLater = new Date(now.getTime() + 24 * 60 * 60 * 1000L);
        assertTrue(compare(SERIALISER.serialise(now), SERIALISER.serialise(aDayLater)) < 0);
    }

    private static int compare(final byte[] first, final byte[] second) {
        for (int i = 0; i < first.length; i++) {
            if (first[i] < second[i]) {
                return -1;
            } else if (first[i] > second[i]) {
                return 1;
            }
        }
        return 0;
    }
}
