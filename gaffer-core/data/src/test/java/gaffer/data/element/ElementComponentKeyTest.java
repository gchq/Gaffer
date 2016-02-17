/*
 * Copyright 2016 Crown Copyright
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

package gaffer.data.element;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class ElementComponentKeyTest {

    @Test
    public void shouldSetAndGetFields() {
        // Given
        final String key = "key 1";
        final ElementComponentKey elmKey = new ElementComponentKey();

        // When
        elmKey.setKey(key);
        elmKey.setIsId(true);

        // When / Then
        assertEquals(key, elmKey.getKey());
        assertTrue(elmKey.isId());
    }

    @Test
    public void shouldGetIdentifierTypeWhenId() {
        // Given
        final IdentifierType idType = IdentifierType.DIRECTED;
        final ElementComponentKey elmKey = new ElementComponentKey(idType.name(), true);


        // When
        final IdentifierType idTypeResult = elmKey.getIdentifierType();

        // Then
        assertEquals(idType, idTypeResult);
    }

    @Test
    public void shouldGetPropertyNameWhenId() {
        // Given
        final String propertyName = "some key";
        final ElementComponentKey elmKey = new ElementComponentKey(propertyName, false);

        // When
        final String propertyNameResult = elmKey.getPropertyName();

        // Then
        assertEquals(propertyName, propertyNameResult);
    }


    @Test
    public void shouldThrowExceptionIfGetIdentifierTypeCalledWhenNotId() {
        // Given
        final ElementComponentKey elmKey = new ElementComponentKey("some key", false);


        // When / Then
        try {
            elmKey.getIdentifierType();
            fail("Exception expected");
        } catch (IllegalStateException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldThrowExceptionIfGetPropertyNameCalledWhenId() {
        // Given
        final ElementComponentKey elmKey = new ElementComponentKey(IdentifierType.DIRECTED.name(), true);


        // When / Then
        try {
            elmKey.getPropertyName();
            fail("Exception expected");
        } catch (IllegalStateException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldGetIdentifierTypeWhenNotId() {
        // Given
        final IdentifierType idType = IdentifierType.DIRECTED;
        final ElementComponentKey elmKey = new ElementComponentKey(idType.name(), true);


        // When
        final IdentifierType idTypeResult = elmKey.getIdentifierType();

        // Then
        assertEquals(idType, idTypeResult);
    }

    @Test
    public void shouldGetIdentifierTypeWhenIdWhenGetKeyObjectCalled() {
        // Given
        final IdentifierType idType = IdentifierType.DIRECTED;
        final ElementComponentKey elmKey = new ElementComponentKey(idType.name(), true);


        // When
        final Object idTypeResult = elmKey.getKeyObject();

        // Then
        assertEquals(idType, idTypeResult);
    }

    @Test
    public void shouldGetPropertyNameWhenNotIdWhenGetKeyObjectCalled() {
        // Given
        final String propertyName = "some key";
        final ElementComponentKey elmKey = new ElementComponentKey(propertyName, false);

        // When
        final Object propertyNameResult = elmKey.getKeyObject();

        // Then
        assertEquals(propertyName, propertyNameResult);
    }

    @Test
    public void shouldNotBeEqualAndNotHaveTheSameHashCodesWhenKeysAreNotTheSameIdType() {
        // Given
        final ElementComponentKey elmKey1 = new ElementComponentKey(IdentifierType.DIRECTED.name(), true);
        final ElementComponentKey elmKey2 = new ElementComponentKey(IdentifierType.VERTEX.name(), true);

        // When / Then
        assertNotEquals(elmKey1, elmKey2);
        assertNotEquals(elmKey1.hashCode(), elmKey2.hashCode());
    }

    @Test
    public void shouldNotBeEqualAndNotHaveTheSameHashCodesWhenKeysAreNotTheSamePropertyName() {
        // Given
        final ElementComponentKey elmKey1 = new ElementComponentKey("propertyName 1", false);
        final ElementComponentKey elmKey2 = new ElementComponentKey("propertyName 2", false);

        // When / Then
        assertNotEquals(elmKey1, elmKey2);
        assertNotEquals(elmKey1.hashCode(), elmKey2.hashCode());
    }

    @Test
    public void shouldNotBeEqualAndNotHaveTheSameHashCodesWhenIsIdFlagIsDifferent() {
        // Given
        final ElementComponentKey elmKey1 = new ElementComponentKey("propertyName 1", false);
        final ElementComponentKey elmKey2 = new ElementComponentKey("propertyName 2", true);

        // When / Then
        assertNotEquals(elmKey1, elmKey2);
        assertNotEquals(elmKey1.hashCode(), elmKey2.hashCode());
    }

    @Test
    public void shouldBeEqualAndHaveTheSameHashCodesWhenKeysAreTheSameIdType() {
        // Given
        final IdentifierType idType = IdentifierType.DIRECTED;
        final ElementComponentKey elmKey1 = new ElementComponentKey(idType.name(), true);
        final ElementComponentKey elmKey2 = new ElementComponentKey(idType.name(), true);

        // When / Then
        assertEquals(elmKey1, elmKey2);
        assertEquals(elmKey1.hashCode(), elmKey2.hashCode());
    }

    @Test
    public void shouldBeEqualAndHaveTheSameHashCodesWhenKeysAreTheSamePropertyName() {
        // Given
        final String propertyName = "some key";
        final ElementComponentKey elmKey1 = new ElementComponentKey(propertyName, false);
        final ElementComponentKey elmKey2 = new ElementComponentKey(propertyName, false);

        // When / Then
        assertEquals(elmKey1, elmKey2);
        assertEquals(elmKey1.hashCode(), elmKey2.hashCode());
    }

    @Test
    public void shouldCreateKeysFromPropertyNames() {
        // Given
        final String property1 = "property 1";
        final String property2 = "property 2";
        final String[] propertyNames = {property1, property2};

        // When
        final ElementComponentKey[] keys = ElementComponentKey.createKeys(propertyNames);

        // When / Then
        assertArrayEquals(new ElementComponentKey[]{new ElementComponentKey(property1), new ElementComponentKey(property2)}, keys);
    }

    @Test
    public void shouldCreateKeysFromIdentifierTypes() {
        // Given
        final IdentifierType idType1 = IdentifierType.SOURCE;
        final IdentifierType idType2 = IdentifierType.DESTINATION;
        final IdentifierType[] propertyNames = {idType1, idType2};

        // When
        final ElementComponentKey[] keys = ElementComponentKey.createKeys(propertyNames);

        // When / Then
        assertArrayEquals(new ElementComponentKey[]{new ElementComponentKey(idType1), new ElementComponentKey(idType2)}, keys);
    }
}
