package uk.gov.gchq.gaffer.serialisation.implementation;

import org.junit.Assert;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

/**
 * Created on 26/06/2017.
 */
public class BooleanSerialiserTest extends ToBytesSerialisationTest<Boolean> {

    @Override
    public Serialiser<Boolean, byte[]> getSerialisation() {
        return new BooleanSerialiser();
    }

    @SuppressWarnings("unchecked")
    public Pair<Boolean, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair(false, new byte[]{0}),
                new Pair(true, new byte[]{1})
        };
    }

    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        Assert.assertFalse(serialiser.deserialiseEmpty());
    }
}