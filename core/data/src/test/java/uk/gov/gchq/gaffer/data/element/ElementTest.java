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

package uk.gov.gchq.gaffer.data.element;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public abstract class ElementTest {

    @Test
    public abstract void shouldReturnTrueForEqualsWhenAllCoreFieldsAreEqual();

    protected abstract Element newElement(final String group);

    protected abstract Element newElement();

    @Test
    public void shouldSetAndGetFields() {
        // Given
        final String group = "group";
        final Properties properties = new Properties();
        final Element element = newElement();

        // When
        element.setGroup(group);
        element.setProperties(properties);

        // Then
        assertEquals(group, element.getGroup());
        assertSame(properties, element.getProperties());
    }

    @Test
    public void shouldCreateElementWithUnknownGroup() {
        // Given
        // When
        final Element element = newElement();

        // Then
        assertEquals(Element.DEFAULT_GROUP, element.getGroup());
    }

    @Test
    public void shouldCreateElementWithGroup() {
        // Given
        final String group = "group";

        // When
        final Element element = newElement(group);

        // Then
        assertEquals("group", element.getGroup());
    }

    @Test
    public void shouldReturnTrueForEqualsWithTheSameInstance() {
        // Given
        final Element element = newElement("group");

        // When
        boolean isEqual = element.equals(element);

        // Then
        assertTrue(isEqual);
        assertEquals(element.hashCode(), element.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenGroupIsDifferent() {
        // Given
        final Element element1 = newElement("group");
        final Object element2 = newElement("a different group");

        // When
        boolean isEqual = element1.equals(element2);

        // Then
        assertFalse(isEqual);
        assertFalse(element1.hashCode() == element2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWithNullObject() {
        // Given
        final Element element1 = newElement("group");

        // When
        boolean isEqual = element1.equals((Object) null);

        // Then
        assertFalse(isEqual);
    }

    @Test
    public void shouldReturnFalseForEqualsWithNullElement() {
        // Given
        final Element element1 = newElement("group");

        // When
        boolean isEqual = element1.equals(null);

        // Then
        assertFalse(isEqual);
    }

    @Test
    public void shouldReturnItselfForGetElement() {
        // Given
        final Element element = newElement("group");

        // When
        final Element result = element.getElement();

        // Then
        assertSame(element, result);
    }

    @Test
    public void shouldCopyProperties() {
        // Given
        final Element element1 = newElement("group");
        final Properties newProperties = new Properties("property1", "propertyValue1");

        // When
        element1.copyProperties(newProperties);

        // Then
        assertEquals(1, element1.getProperties().size());
        assertEquals("propertyValue1", element1.getProperty("property1"));
    }

    @Test
    public void shouldSerialiseAndDeserialiseProperties() throws SerialisationException {
        // Given
        final Element element = newElement("group");
        final Properties properties = new Properties();
        properties.put("property1", 1L);
        properties.put("property2", 2);
        properties.put("property3", (double) 3);
        properties.put("property4", "4");
        properties.put("property5", new Date(5L));
        element.setProperties(properties);

        final JSONSerialiser serialiser = new JSONSerialiser();

        // When
        final byte[] serialisedElement = serialiser.serialise(element);
        final Element deserialisedElement = serialiser.deserialise(serialisedElement, element.getClass());

        // Then
        assertEquals(element, deserialisedElement);
    }

    @Test
    public abstract void shouldSerialiseAndDeserialiseIdentifiers() throws SerialisationException;
}
