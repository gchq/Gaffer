package uk.gov.gchq.gaffer.sketches.serialisation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;

/**
 * Created on 27/06/2017.
 */
public abstract class ViaCalculatedArrayValueSerialiserTest<OUTPUT, VALUE> extends ViaCalculatedValueSerialiserTest<OUTPUT, VALUE[]> {

    @Override
    protected void serialiseFirst(final Pair<OUTPUT, byte[]> pair) throws SerialisationException {
        final byte[] serialised = serialiser.serialise(pair.getFirst());
        final VALUE[] estimateFirstValue = useTestValue(pair.getFirst());
        final VALUE[] estimateFirstValueDeserialised = useTestValue(serialiser.deserialise(serialised));
        assertArrayEquals(estimateFirstValue, estimateFirstValueDeserialised);
        final VALUE[] estimateSecondValueDeserialised = useTestValue(serialiser.deserialise(pair.getSecond()));
        assertArrayEquals(estimateFirstValue, estimateSecondValueDeserialised);
    }

    @Override
    protected void testSerialiser(final OUTPUT object) {
        try {

            int countOfNullPointer = 0;

            VALUE[] originalValue;
            try {
                originalValue = useTestValue(object);
                countOfNullPointer++;
            } catch (NullPointerException e) {
                originalValue = null;
            }

            VALUE[] deserialisedValue;
            try {
                final OUTPUT objectDeserialised = serialiser.deserialise(serialiser.serialise(object));
                deserialisedValue = useTestValue(objectDeserialised);
                countOfNullPointer++;
            } catch (NullPointerException e) {
                deserialisedValue = null;
            }

            assertArrayEquals(originalValue, deserialisedValue);
            assertTrue("The values are equal, however one of them was assigned null via NullPointerException the other wasn't.", countOfNullPointer != 1);


        } catch (final SerialisationException exception) {
            fail("A SerialisationException occurred");
        }
    }
}
