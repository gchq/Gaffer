package uk.gov.gchq.gaffer.types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeSubTypeValueTest {

    @Test
    public void testComparisonsAreAsExpected() {
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue("a", "b", "c");

        assertEquals(0, typeSubTypeValue.compareTo(new TypeSubTypeValue("a", "b", "c")));

        assertTrue(typeSubTypeValue.compareTo(null) > 0);

        assertTrue(typeSubTypeValue.compareTo(new TypeSubTypeValue()) > 0);

        assertTrue(typeSubTypeValue.compareTo(new TypeSubTypeValue("1", "b", "c")) > 0);

        assertTrue(typeSubTypeValue.compareTo(new TypeSubTypeValue("a", "a", "c")) > 0);

        assertTrue(typeSubTypeValue.compareTo(new TypeSubTypeValue("a", "b", "a")) > 0);

        assertTrue(typeSubTypeValue.compareTo(new TypeSubTypeValue("b", "a", "c")) < 0);

        assertTrue(typeSubTypeValue.compareTo(new TypeSubTypeValue("a", "c", "c")) < 0);

        assertTrue(typeSubTypeValue.compareTo(new TypeSubTypeValue("a", "b", "d")) < 0);
    }

    @Test
    public void testHashCodeAndEqualsMethodTreatsEmptyStringAsNull() {
        // Given
        TypeSubTypeValue typeSubTypeValueEmptyStrings = new TypeSubTypeValue("", "", "X");
        TypeSubTypeValue typeSubTypeValueNullStrings = new TypeSubTypeValue(null, null, "X");

        // When
        Boolean equalsResult = typeSubTypeValueEmptyStrings.equals(typeSubTypeValueNullStrings);

        // Then
        assertTrue(equalsResult);
        assertEquals(typeSubTypeValueEmptyStrings.hashCode(), typeSubTypeValueNullStrings.hashCode());
    }
}
