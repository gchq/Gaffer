package uk.gov.gchq.gaffer.types.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.tuple.n.Tuple2;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StringsToTypeValueTest extends FunctionTest {
    @Test
    public void shouldConvertStringToTypeValue() {
        // Given
        final StringsToTypeValue function = new StringsToTypeValue();

        final String type = "type1";
        final String value = "value1";

        // When
        final TypeValue result = function.apply(new Tuple2<>(type, value));

        // Then
        assertEquals(new TypeValue(type, value), result);
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringsToTypeValue function = new StringsToTypeValue();

        // When 1
        final String json = new String(JSONSerialiser.serialise(function, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.StringsToTypeValue\"%n" +
                "}"), json);

        // When 2
        final StringsToTypeValue deserialisedFunction = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedFunction);
    }

    @Override
    protected StringsToTypeValue getInstance() {
        return new StringsToTypeValue();
    }

    @Override
    protected Class<StringsToTypeValue> getFunctionClass() {
        return StringsToTypeValue.class;
    }
}
