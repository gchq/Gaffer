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

package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StorePropertiesTest {

    @Test
    public void shouldRemovePropertyWhenPropertyValueIsNull() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        props.setId(null);

        // Then
        assertNull(props.getId());
    }

    @Test
    public void shouldGetProperty() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        String value = props.get("key1");

        // Then
        assertEquals("value1", value);
    }

    @Test
    public void shouldSetAndGetProperty() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        props.set("key2", "value2");
        String value = props.get("key2");

        // Then
        assertEquals("value2", value);
    }

    @Test
    public void shouldGetPropertyWithDefaultValue() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        String value = props.get("key1", "property not found");

        // Then
        assertEquals("value1", value);
    }

    @Test
    public void shouldGetUnknownProperty() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        String value = props.get("a key that does not exist");

        // Then
        assertNull(value);
    }

    @Test
    public void shouldAddOperationDeclarationPathsWhenNullExisting() {
        // Given
        final StoreProperties props = createStoreProperties();
        assertNull(props.getOperationDeclarationPaths());

        // When
        props.addOperationDeclarationPaths("1", "2");

        // Then
        assertEquals("1,2", props.getOperationDeclarationPaths());
    }

    @Test
    public void shouldAddOperationDeclarationPathsWhenExisting() {
        // Given
        final StoreProperties props = createStoreProperties();
        props.setOperationDeclarationPaths("1");

        // When
        props.addOperationDeclarationPaths("2", "3");

        // Then
        assertEquals("1,2,3", props.getOperationDeclarationPaths());
    }

    @Test
    public void shouldGetUnknownPropertyWithDefaultValue() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        String value = props.get("a key that does not exist", "property not found");

        // Then
        assertEquals("property not found", value);
    }

    private StoreProperties createStoreProperties() {
        return StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
    }

    @Test
    public void shouldSetJsonSerialiserModules() {
        // Given
        final StoreProperties props = createStoreProperties();
        final Set<Class<? extends JSONSerialiserModules>> modules = Sets.newHashSet(
                TestCustomJsonModules1.class,
                TestCustomJsonModules2.class
        );

        // When
        props.setJsonSerialiserModules(modules);

        // Then
        assertEquals(
                TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName(),
                props.getJsonSerialiserModules()
        );
    }

    public static final class TestCustomJsonModules1 implements JSONSerialiserModules {
        public static List<Module> modules;

        @Override
        public List<Module> getModules() {
            return modules;
        }
    }

    public static final class TestCustomJsonModules2 implements JSONSerialiserModules {
        public static List<Module> modules;

        @Override
        public List<Module> getModules() {
            return modules;
        }
    }
}
