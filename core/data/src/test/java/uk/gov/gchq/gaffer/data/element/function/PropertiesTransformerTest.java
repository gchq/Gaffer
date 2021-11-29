package uk.gov.gchq.gaffer.data.element.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class PropertiesTransformerTest extends FunctionTest {

    @Test
    void shouldCreateEmptySetWhenNull() {
        // Given
        final PropertiesTransformer propertiesTransformer =
                new PropertiesTransformer();
        Properties properties = new Properties("Test","Property");
        // When
        Properties result = propertiesTransformer.apply(properties);

        // Then
       PropertiesTransformer expected = new PropertiesTransformer.Builder()
               .select("Test")
               .execute(Object::toString)
                .build();

        assertEquals(expected, result);

    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Properties.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Properties.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final PropertiesTransformer propertiesTransformer =
                new PropertiesTransformer();
        // When
        final String json = new String(JSONSerialiser.serialise(propertiesTransformer));
        PropertiesTransformer deserialisedPropertiesTransformer = JSONSerialiser.deserialise(json, PropertiesTransformer.class);
        // Then
        assertEquals(propertiesTransformer, deserialisedPropertiesTransformer);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.ToTimestampSet\",\"bucket\":\"DAY\",\"millisCorrection\":1}", json );

    }

    @Override
    protected Object getInstance() {
        return new PropertiesTransformer();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}