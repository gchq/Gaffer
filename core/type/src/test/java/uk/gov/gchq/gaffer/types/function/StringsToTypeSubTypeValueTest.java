package uk.gov.gchq.gaffer.types.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.tuple.ArrayTuple;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StringsToTypeSubTypeValueTest extends FunctionTest {
    @Test
    public void shouldConvertStringToTypeSubTypeValue() {
        // Given
        final StringsToTypeSubTypeValue function = new StringsToTypeSubTypeValue();

        final String type = "type1";
        final String subtype = "subType1";
        final String value = "value1";

        // When
        final TypeSubTypeValue result = function.apply(new Tuple3<>(type, subtype, value));

        // Then
        assertEquals(new TypeSubTypeValue(type, subtype, value), result);
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringsToTypeSubTypeValue function = new StringsToTypeSubTypeValue();

        // When 1
        final String json = new String(JSONSerialiser.serialise(function, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.StringsToTypeSubTypeValue\"%n" +
                "}"), json);

        // When 2
        final StringsToTypeSubTypeValue deserialisedFunction = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedFunction);
    }

    @Override
    protected StringsToTypeSubTypeValue getInstance() {
        return new StringsToTypeSubTypeValue();
    }

    @Override
    protected Class<StringsToTypeSubTypeValue> getFunctionClass() {
        return StringsToTypeSubTypeValue.class;
    }
}
