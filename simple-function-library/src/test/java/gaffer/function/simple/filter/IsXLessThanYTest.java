package gaffer.function.simple.filter;

import gaffer.exception.SerialisationException;
import gaffer.function.FilterFunctionTest;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class IsXLessThanYTest extends FilterFunctionTest {
    @Test
    public void shouldAcceptWhenLessThan() {
        // Given
        final IsXLessThanY filter = new IsXLessThanY();

        // When
        boolean accepted = filter._isValid(new Object[]{1, 2});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenMoreThan() {
        // Given
        final IsXLessThanY filter = new IsXLessThanY();

        // When
        boolean accepted = filter._isValid(new Object[]{6, 5});

        // Then
        assertFalse(accepted);
    }


    @Test
    public void shouldRejectTheValueWhenEqualTo() {
        // Given
        final IsXLessThanY filter = new IsXLessThanY();

        // When
        boolean accepted = filter._isValid(new Object[]{5, 5});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final IsXLessThanY filter = new IsXLessThanY();

        // When
        final IsXLessThanY clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final IsXLessThanY filter = new IsXLessThanY();

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.filter.IsXLessThanY\"\n" +
                "}", json);

        // When 2
        final IsXLessThanY deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), IsXLessThanY.class);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

    @Override
    protected Class<IsXLessThanY> getFunctionClass() {
        return IsXLessThanY.class;
    }

    @Override
    protected IsXLessThanY getInstance() {
        return new IsXLessThanY();
    }

    @Override
    protected Object[] getSomeAcceptedInput() {
        return new Object[]{1, 2};
    }
}
