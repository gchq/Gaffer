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

package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StorePropertiesTest {

    @Before
    @After
    public void cleanUp() {
        ReflectionUtil.resetReflectionPackages();
    }

    @Test
    public void shouldMergeProperties() {
        // Given
        final StoreProperties props1 = createStoreProperties();
        final StoreProperties props2 = StoreProperties.loadStoreProperties(StreamUtil.openStream(getClass(), "store2.properties"));

        // When
        props1.merge(props2);

        // Then
        assertEquals("value1", props1.getProperty("key1"));
        assertEquals("value2", props1.getProperty("key2"));
        assertEquals("value2", props1.getProperty("testKey"));
    }

    @Test
    public void shouldRemovePropertyWhenPropertyValueIsNull() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        props.setProperty("testKey", null);

        // Then
        assertNull(props.getProperty("testKey"));
    }

    @Test
    public void shouldGetProperty() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        String value = props.getProperty("key1");

        // Then
        assertEquals("value1", value);
    }

    @Test
    public void shouldSetAndGetProperty() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        props.setProperty("key2", "value2");
        String value = props.getProperty("key2");

        // Then
        assertEquals("value2", value);
    }

    @Test
    public void shouldGetPropertyWithDefaultValue() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        String value = props.getProperty("key1", "property not found");

        // Then
        assertEquals("value1", value);
    }

    @Test
    public void shouldGetUnknownProperty() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        String value = props.getProperty("a key that does not exist");

        // Then
        assertNull(value);
    }

    @Test
    public void shouldAddOperationDeclarationPathsWhenNullExisting() {
        // Given
        final StoreProperties props = createStoreProperties();
        assertNull(StorePropertiesUtil.getOperationDeclarationPaths(props));

        // When
        StorePropertiesUtil.addOperationDeclarationPaths(props, "1", "2");

        // Then
        assertEquals("1,2", StorePropertiesUtil.getOperationDeclarationPaths(props));
    }

    @Test
    public void shouldAddOperationDeclarationPathsWhenExisting() {
        // Given
        final StoreProperties props = createStoreProperties();
        StorePropertiesUtil.setOperationDeclarationPaths(props, "1");

        // When
        StorePropertiesUtil.addOperationDeclarationPaths(props, "2", "3");

        // Then
        assertEquals("1,2,3", StorePropertiesUtil.getOperationDeclarationPaths(props));
    }

    @Test
    public void shouldAddReflectionPackagesToKorypheReflectionUtil() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        StorePropertiesUtil.setReflectionPackages(props, "package1,package2");

        // Then
        assertEquals("package1,package2", StorePropertiesUtil.getReflectionPackages(props));
        final Set<String> expectedPackages = Sets.newHashSet(ReflectionUtil.DEFAULT_PACKAGES);
        expectedPackages.add("package1");
        expectedPackages.add("package2");
        assertEquals(expectedPackages, ReflectionUtil.getReflectionPackages());
    }

    @Test
    public void shouldGetUnknownPropertyWithDefaultValue() {
        // Given
        final StoreProperties props = createStoreProperties();

        // When
        String value = props.getProperty("a key that does not exist", "property not found");

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
        StorePropertiesUtil.setJsonSerialiserModules(props, modules);

        // Then
        assertEquals(
                TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName(),
                props.getProperty(StorePropertiesUtil.JSON_SERIALISER_MODULES)
        );
    }

    @Test
    public void shouldGetAndSetAdminAuth() {
        // Given
        final String adminAuth = "admin auth";
        final StoreProperties props = createStoreProperties();

        // When
        StorePropertiesUtil.setAdminAuth(props, adminAuth);

        // Then
        assertEquals(adminAuth, StorePropertiesUtil.getAdminAuth(props));
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
