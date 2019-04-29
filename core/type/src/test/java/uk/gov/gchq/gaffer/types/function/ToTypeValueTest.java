package uk.gov.gchq.gaffer.types.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.function.FunctionTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ToTypeValueTest extends FunctionTest {
    @Test
    public void shouldConvertStringToTypeValue() {
        // Given
        final ToTypeValue function = new ToTypeValue();

        final Object value = "value1";

        // When
        final TypeValue result = function.apply(value);

        // Then
        assertEquals(new TypeValue(null, value.toString()), result);
    }

    @Test
    public void shouldConvertObjectToTypeValue() {
        // Given
        final ToTypeValue function = new ToTypeValue();

        final Object value = 1L;

        // When
        final TypeValue result = function.apply(value);

        // Then
        assertEquals(new TypeValue(null, value.toString()), result);
    }

    @Test
    public void shouldConvertNullToTypeValue() {
        // Given
        final ToTypeValue function = new ToTypeValue();

        final Object value = null;

        // When
        final TypeValue result = function.apply(value);

        // Then
        assertEquals(new TypeValue(null, null), result);
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ToTypeValue function = new ToTypeValue();

        // When 1
        final String json = new String(JSONSerialiser.serialise(function, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.ToTypeValue\"%n" +
                "}"), json);

        // When 2
        final ToTypeValue deserialisedFunction = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedFunction);
    }

    @Override
    protected ToTypeValue getInstance() {
        return new ToTypeValue();
    }

    @Override
    protected Class<ToTypeValue> getFunctionClass() {
        return ToTypeValue.class;
    }
}
