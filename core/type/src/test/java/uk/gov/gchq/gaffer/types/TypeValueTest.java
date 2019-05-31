/*
 * Copyright 2016-2019 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
