package uk.gov.gchq.gaffer.types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeValueTest {

    @Test
    public void testComparisonsAreAsExpected() {
        TypeValue typeValue = new TypeValue("a", "b");

        assertEquals(0, typeValue.compareTo(new TypeValue("a", "b")));

        assertTrue(typeValue.compareTo(null) > 0);

        assertTrue(typeValue.compareTo(new TypeValue()) > 0);

        assertTrue(typeValue.compareTo(new TypeValue("1", "b")) > 0);

        assertTrue(typeValue.compareTo(new TypeValue("a", "a")) > 0);

        assertTrue(typeValue.compareTo(new TypeValue("b", "a")) < 0);

        assertTrue(typeValue.compareTo(new TypeValue("a", "c")) < 0);
    }

    @Test
    public void testHashCodeAndEqualsMethodTreatsEmptyStringAsNull() {
        // Given
        TypeValue typeValueEmptyStrings = new TypeValue("", "");
        TypeValue typeValueNullStrings = new TypeValue(null, null);

        // When
        boolean equalsResult = typeValueEmptyStrings.equals(typeValueNullStrings);

        // Then
        assertTrue(equalsResult);
        assertEquals(typeValueEmptyStrings.hashCode(), typeValueNullStrings.hashCode());
    }
}
