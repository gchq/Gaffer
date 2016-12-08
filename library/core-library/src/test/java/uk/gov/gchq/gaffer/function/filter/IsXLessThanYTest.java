package uk.gov.gchq.gaffer.function.filter;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.FilterFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

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
        boolean accepted = filter.isValid(new Object[]{1, 2});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenMoreThan() {
        // Given
        final IsXLessThanY filter = new IsXLessThanY();

        // When
        boolean accepted = filter.isValid(new Object[]{6, 5});

        // Then
        assertFalse(accepted);
    }


    @Test
    public void shouldRejectTheValueWhenEqualTo() {
        // Given
        final IsXLessThanY filter = new IsXLessThanY();

        // When
        boolean accepted = filter.isValid(new Object[]{5, 5});

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
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.IsXLessThanY\"%n" +
                "}"), json);

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
}