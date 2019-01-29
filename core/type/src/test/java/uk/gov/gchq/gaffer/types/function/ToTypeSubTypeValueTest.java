package uk.gov.gchq.gaffer.types.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.koryphe.function.FunctionTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ToTypeSubTypeValueTest extends FunctionTest {
    @Test
    public void shouldConvertStringToTypeSubTypeValue() {
        // Given
        final ToTypeSubTypeValue function = new ToTypeSubTypeValue();

        final Object value = "value1";

        // When
        final TypeSubTypeValue result = function.apply(value);

        // Then
        assertEquals(new TypeSubTypeValue(null, null, value.toString()), result);
    }

    @Test
    public void shouldConvertObjectToTypeSubTypeValue() {
        // Given
        final ToTypeSubTypeValue function = new ToTypeSubTypeValue();

        final Object value = 1L;

        // When
        final TypeSubTypeValue result = function.apply(value);

        // Then
        assertEquals(new TypeSubTypeValue(null, null, value.toString()), result);
    }

    @Test
    public void shouldConvertNullToTypeSubTypeValue() {
        // Given
        final ToTypeSubTypeValue function = new ToTypeSubTypeValue();

        final Object value = null;

        // When
        final TypeSubTypeValue result = function.apply(value);

        // Then
        assertEquals(new TypeSubTypeValue(null, null, null), result);
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ToTypeSubTypeValue function = new ToTypeSubTypeValue();

        // When 1
        final String json = new String(JSONSerialiser.serialise(function, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.ToTypeSubTypeValue\"%n" +
                "}"), json);

        // When 2
        final ToTypeSubTypeValue deserialisedFunction = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedFunction);
    }

    @Override
    protected ToTypeSubTypeValue getInstance() {
        return new ToTypeSubTypeValue();
    }

    @Override
    protected Class<ToTypeSubTypeValue> getFunctionClass() {
        return ToTypeSubTypeValue.class;
    }
}
