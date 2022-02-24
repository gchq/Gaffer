/*
 * Copyright 2016-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EntityTest extends ElementTest {

    @Test
    @Override
    public void shouldSetAndGetFields() {
        final Entity entity = new Entity.Builder()
                .group("group")
                .vertex("identifier")
                .build();

        assertEquals("group", entity.getGroup());
        assertEquals("identifier", entity.getVertex());
    }

    @Test
    public void shouldBuildEntity() {
        final String vertex = "vertex1";
        final String propValue = "propValue";

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(vertex)
                .property(TestPropertyNames.STRING, propValue)
                .build();

        assertEquals(TestGroups.ENTITY, entity.getGroup());
        assertEquals(vertex, entity.getVertex());
        assertEquals(propValue, entity.getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldConstructEntity() {
        final String vertex = "vertex1";
        final String propValue = "propValue";

        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        entity.putProperty(TestPropertyNames.STRING, propValue);

        assertEquals(TestGroups.ENTITY, entity.getGroup());
        assertEquals(vertex, entity.getVertex());
        assertEquals(propValue, entity.getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldCloneEntity() {
        final String vertex = "vertex1";

        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        final Entity clone = entity.emptyClone();

        assertEquals(clone, entity);
    }

    @Test
    @Override
    public void shouldReturnTrueForEqualsWithTheSameInstance() {
        final Entity entity = new Entity("group");
        entity.setVertex("identifier");

        boolean isEqual = entity.equals(entity);

        assertTrue(isEqual);
        assertEquals(entity.hashCode(), entity.hashCode());
    }

    @Test
    @Override
    public void shouldReturnTrueForEqualsWhenAllCoreFieldsAreEqual() {
        // Given
        final Entity entity1 = new Entity("group");
        entity1.setVertex("identifier");
        entity1.putProperty("some property", "some value");

        final Entity entity2 = cloneCoreFields(entity1);
        entity2.putProperty("some property", "some value");

        // When
        boolean isEqual = entity1.equals((Object) entity2);

        // Then
        assertTrue(isEqual);
        assertEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenPropertyIsDifferent() {
        // Given
        final Entity entity1 = new Entity("group");
        entity1.setVertex("identifier");
        entity1.putProperty("some property", "some value");

        final Entity entity2 = cloneCoreFields(entity1);
        entity2.putProperty("some property", "some other value");

        // When
        boolean isEqual = entity1.equals((Object) entity2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Test
    @Override
    public void shouldReturnFalseForEqualsWhenGroupIsDifferent() {
        // Given
        final Entity entity1 = new Entity("group");
        entity1.setVertex("vertex");

        final Entity entity2 = new Entity("a different group");
        entity2.setVertex(entity1.getVertex());

        // When
        boolean isEqual = entity1.equals((Object) entity2);

        // Then
        assertFalse(isEqual);
        assertFalse(entity1.hashCode() == entity2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenIdentifierIsDifferent() {
        final Entity entity1 = new Entity("group");
        entity1.setVertex("vertex");
        final Entity entity2 = cloneCoreFields(entity1);
        entity2.setVertex("different vertex");

        boolean isEqual = entity1.equals((Object) entity2);

        assertFalse(isEqual);
        assertFalse(entity1.hashCode() == entity2.hashCode());
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseIdentifiers() throws SerialisationException {
        final Entity entity = newElement("group");
        entity.setVertex(1L);

        final byte[] serialisedElement = JSONSerialiser.serialise(entity);
        final Entity deserialisedElement = JSONSerialiser.deserialise(serialisedElement, entity.getClass());

        assertEquals(entity, deserialisedElement);
        assertTrue(StringUtil.toString(serialisedElement).contains("{\"java.lang.Long\":1}"));
    }

    @Override
    protected Entity newElement(final String group) {
        return new Entity(group);
    }

    @Override
    protected Entity newElement() {
        return new Entity.Builder().build();
    }

    private Entity cloneCoreFields(final Entity entity) {
        final Entity newEntity = new Entity(entity.getGroup());
        newEntity.setVertex(entity.getVertex());

        return newEntity;
    }
}
