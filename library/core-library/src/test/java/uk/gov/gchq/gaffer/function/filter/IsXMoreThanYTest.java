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

public class IsXMoreThanYTest extends FilterFunctionTest {
    @Test
    public void shouldAcceptWhenMoreThan() {
        // Given
        final IsXMoreThanY filter = new IsXMoreThanY();

        // When
        boolean accepted = filter.isValid(new Object[]{2, 1});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenMoreThan() {
        // Given
        final IsXMoreThanY filter = new IsXMoreThanY();

        // When
        boolean accepted = filter.isValid(new Object[]{5, 6});

        // Then
        assertFalse(accepted);
    }


    @Test
    public void shouldRejectTheValueWhenEqualTo() {
        // Given
        final IsXMoreThanY filter = new IsXMoreThanY();

        // When
        boolean accepted = filter.isValid(new Object[]{5, 5});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final IsXMoreThanY filter = new IsXMoreThanY();

        // When
        final IsXMoreThanY clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final IsXMoreThanY filter = new IsXMoreThanY();

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.IsXMoreThanY\"%n" +
                "}"), json);

        // When 2
        final IsXMoreThanY deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), IsXMoreThanY.class);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

    @Override
    protected Class<IsXMoreThanY> getFunctionClass() {
        return IsXMoreThanY.class;
    }

    @Override
    protected IsXMoreThanY getInstance() {
        return new IsXMoreThanY();
    }
}
