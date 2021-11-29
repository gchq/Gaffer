package uk.gov.gchq.gaffer.data.element.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.function.IterableToFreqMap;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.tuple.predicate.KoryphePredicate2;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class PropertiesFilterTest extends JSONSerialisationTest<PropertiesFilter> {

    @Test
    public void shouldTestPropertiesOnPredicate2() {
        // Given
        final PropertiesFilter filter = new PropertiesFilter.Builder()
                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .execute(new KoryphePredicate2<String, String>() {
                    @Override
                    public boolean test(final String o, final String o2) {
                        return "value".equals(o) && "value2".equals(o2);
                    }
                })
                .build();

        final Properties properties1 = new Properties();
        final Properties properties2 = new Properties();
        properties1.put(TestPropertyNames.PROP_1, "value");
        properties1.put(TestPropertyNames.PROP_2, "value2");
        properties2.put(TestPropertyNames.PROP_1, "value_incorrect");
        properties2.put(TestPropertyNames.PROP_2, "value2_incorrect");

        // When
        final boolean result1 = filter.test(properties1);
        final boolean result2 = filter.test(properties2);

        // Then
        assertTrue(result1);
        assertFalse(result2);
    }

    @Test
    public void shouldTestPropertiesOnPredicate2WithValidationResult() {
        // Given
        final KoryphePredicate2<String, String> predicate2 = new KoryphePredicate2<String, String>() {
            @Override
            public boolean test(final String o, final String o2) {
                return "value".equals(o) && "value2".equals(o2);
            }
        };
        final PropertiesFilter filter = new PropertiesFilter.Builder()
                .select("prop1", "prop2")
                .execute(predicate2)
                .build();


        final Properties properties1 = new Properties();
        final Properties properties2 = new Properties();
        properties1.put("prop1", "value");
        properties1.put("prop2", "value2");
        properties2.put("prop1", "value_incorrect");
        properties2.put("prop2", "value2_incorrect");

        // When
        final ValidationResult result1 = filter.testWithValidationResult(properties1);
        final ValidationResult result2 = filter.testWithValidationResult(properties2);

        // Then
        assertTrue(result1.isValid());
        assertFalse(result2.isValid());
        assertTrue(result2.getErrorString().contains("{prop1: <java.lang.String>value_incorrect, prop2: <java.lang.String>value2_incorrect}"), "Result was: " + result2.getErrorString());
    }


    @Override
    protected PropertiesFilter getTestObject() {
        return new PropertiesFilter();
    }
}