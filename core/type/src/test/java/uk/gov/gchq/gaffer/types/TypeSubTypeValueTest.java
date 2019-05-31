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
