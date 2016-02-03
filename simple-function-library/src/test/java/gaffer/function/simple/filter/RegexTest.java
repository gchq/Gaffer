package gaffer.function.simple.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import gaffer.exception.SerialisationException;
import gaffer.function.FilterFunction;
import gaffer.function.FilterFunctionTest;
import gaffer.function.Function;
import gaffer.jsonserialisation.JSONSerialiser;

public class RegexTest extends FilterFunctionTest {
    @Test
    public void shouldAccepValidValue() {
        // Given
        final Regex filter = new Regex("te[a-d]{3}st");


        // When
        boolean accepted = filter.filter("teaadst");

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectInvalidValue() {
        // Given
        final Regex filter = new Regex("fa[a-d]{3}il");

        // When
        boolean accepted = filter.filter("favcdil");

        // Then
        assertFalse(accepted);
    }
    
    @Test
    public void shouldClone() {
        // Given
        final Regex filter = new Regex();

        // When
        final Regex clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Regex filter = new Regex("test");

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.filter.Regex\",\n" +
                "  \"value\" : {\n"
                + "    \"java.util.regex.Pattern\" : \"test\"\n"
                + "  }\n" +
                "}", json);

        // When 2
        final Regex deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), Regex.class);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

	@Override
	protected FilterFunction getInstance() {
		return new Regex("[a-zA-Z]{1,12}");
	}

	@Override
	protected Object[] getSomeAcceptedInput() {
        return new Object[]{"suCCeSs"};
	}

	@Override
	protected Class<? extends Function> getFunctionClass() {
		return Regex.class;
	}

}