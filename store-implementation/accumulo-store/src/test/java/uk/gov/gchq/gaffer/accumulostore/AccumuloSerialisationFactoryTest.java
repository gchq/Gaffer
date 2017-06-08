package uk.gov.gchq.gaffer.accumulostore;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.SerialisationFactory;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawIntegerSerialiser;
import uk.gov.gchq.gaffer.sketches.serialisation.HyperLogLogPlusSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AccumuloSerialisationFactoryTest {
    @Test
    public void shouldReturnSerialiserForAString() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new AccumuloSerialisationFactory();
        final Class<?> clazz = String.class;

        // When
        final Serialiser serialiser = factory.getSerialiser(clazz);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(StringSerialiser.class, serialiser.getClass());
    }

    @Test
    public void shouldReturnOrderedSerialiserForAString() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new AccumuloSerialisationFactory();
        final Class<?> clazz = String.class;
        final boolean ordered = true;

        // When
        final Serialiser serialiser = factory.getSerialiser(clazz, ordered);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(StringSerialiser.class, serialiser.getClass());
    }

    @Test
    public void shouldReturnSerialiserForAnInteger() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new AccumuloSerialisationFactory();
        final Class<?> clazz = Integer.class;

        // When
        final Serialiser serialiser = factory.getSerialiser(clazz);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(CompactRawIntegerSerialiser.class, serialiser.getClass());
    }

    @Test
    public void shouldReturnOrderedSerialiserForAnInteger() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new AccumuloSerialisationFactory();
        final Class<?> clazz = Integer.class;
        final boolean ordered = true;

        // When
        final Serialiser serialiser = factory.getSerialiser(clazz, ordered);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(RawIntegerSerialiser.class, serialiser.getClass());
    }

    @Test
    public void shouldThrowExceptionIfClassIsNull() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new AccumuloSerialisationFactory();
        final Class<?> clazz = null;

        // When / Then
        try {
            factory.getSerialiser(clazz);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfNoSerialiserFound() throws SerialisationException {
        // Given
        final SerialisationFactory factory = new AccumuloSerialisationFactory();
        final Class<?> clazz = Object.class;

        // When / Then
        try {
            factory.getSerialiser(clazz);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldReturnSerialiserForHyperLogLogPlus() {
        //Given
        final SerialisationFactory factory = new AccumuloSerialisationFactory();
        final Class<?> clazz = HyperLogLogPlus.class;

        //when
        final Serialiser serialiser = factory.getSerialiser(clazz);

        // Then
        assertTrue(serialiser.canHandle(clazz));
        assertEquals(HyperLogLogPlusSerialiser.class, serialiser.getClass());
    }
}
