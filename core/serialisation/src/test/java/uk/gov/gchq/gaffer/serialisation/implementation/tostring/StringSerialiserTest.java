package uk.gov.gchq.gaffer.serialisation.implementation.tostring;

import org.junit.Assert;
import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.SerialisationTest;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.tostring.StringSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created on 22/05/2017.
 */
public class StringSerialiserTest extends SerialisationTest<String, String> {


    public static final String STRING_VALUE_1 = "StringValue1";

    @Test
    public void shouldSerialiseAndDeserialise() throws Exception {
        final String serialised = serialiser.serialise(STRING_VALUE_1);
        final String deserialise = serialiser.deserialise(serialised);
        assertEquals(STRING_VALUE_1, serialised);
        assertEquals(STRING_VALUE_1, deserialise);
        assertEquals(serialised, deserialise);
    }


    @Override
    public void shouldSerialiseNull() throws SerialisationException {
        assertNull(serialiser.serialiseNull());
    }

    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        Assert.assertNull(serialiser.serialiseNull());
    }

    @Override
    public Serialiser<String, String> getSerialisation() {
        return new StringSerialiser();
    }
}