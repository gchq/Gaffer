/*
 * Copyright 2016-2024 Crown Copyright
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.store.StoreProperties.loadStoreProperties;

public class StorePropertiesTest {

    @BeforeEach
    @AfterEach
    public void cleanUp() {
        ReflectionUtil.resetReflectionPackages();
    }

    @Test
    public void shouldMergeProperties() {
        // Given
        final StoreProperties props1 = loadStoreProperties("store.properties");
        final StoreProperties props2 = loadStoreProperties("store2.properties");

        // When
        props1.merge(props2);

        // Then
        assertThat(props1.get("key1")).isEqualTo("value1");
        assertThat(props1.get("key2")).isEqualTo("value2");
        assertThat(props1.get("testKey")).isEqualTo("value2");
    }

    @Test
    public void shouldRemovePropertyWhenPropertyValueIsNull() {
        // Given
        final StoreProperties props = new StoreProperties();

        // When
        props.set("testKey", null);

        // Then
        assertThat(props.get("testKey")).isNull();
    }

    @Test
    public void shouldGetProperty() {
        // Given
        final StoreProperties props = loadStoreProperties("store.properties");

        // When
        String value = props.get("key1");

        // Then
        assertThat(value).isEqualTo("value1");
    }

    @Test
    public void shouldSetAndGetProperty() {
        // Given
        final StoreProperties props = new StoreProperties();

        // When
        props.set("key2", "value2");
        String value = props.get("key2");

        // Then
        assertThat(value).isEqualTo("value2");
    }

    @Test
    public void shouldGetPropertyWithDefaultValue() {
        // Given
        final StoreProperties props = loadStoreProperties("store.properties");

        // When
        String value = props.get("key1", "property not found");

        // Then
        assertThat(value).isEqualTo("value1");
    }

    @Test
    public void shouldGetUnknownProperty() {
        // Given
        final StoreProperties props = new StoreProperties();

        // When
        String value = props.get("a key that does not exist");

        // Then
        assertThat(value).isNull();
    }

    @Test
    public void shouldAddOperationDeclarationPathsWhenNullExisting() {
        // Given
        final StoreProperties props = new StoreProperties();
        assertThat(props.getOperationDeclarationPaths()).isNull();

        // When
        props.addOperationDeclarationPaths("1", "2");

        // Then
        assertThat(props.getOperationDeclarationPaths()).isEqualTo("1,2");
    }

    @Test
    public void shouldAddOperationDeclarationPathsWhenExisting() {
        // Given
        final StoreProperties props = new StoreProperties();
        props.setOperationDeclarationPaths("1");

        // When
        props.addOperationDeclarationPaths("2", "3");

        // Then
        assertThat(props.getOperationDeclarationPaths()).isEqualTo("1,2,3");
    }

    @Test
    public void shouldAddReflectionPackagesToKorypheReflectionUtil() {
        // Given
        final StoreProperties props = new StoreProperties();

        // When
        props.setReflectionPackages("package1,package2");

        // Then
        assertThat(props.getReflectionPackages()).isEqualTo("package1,package2");
        final Set<String> expectedPackages = new HashSet<>(ReflectionUtil.DEFAULT_PACKAGES);
        expectedPackages.add("package1");
        expectedPackages.add("package2");
        assertThat(ReflectionUtil.getReflectionPackages()).isEqualTo(expectedPackages);
    }

    @Test
    public void shouldGetUnknownPropertyWithDefaultValue() {
        // Given
        final StoreProperties props = new StoreProperties();

        // When
        String value = props.get("a key that does not exist", "property not found");

        // Then
        assertThat(value).isEqualTo("property not found");
    }

    @Test
    public void shouldSetJsonSerialiserModules() {
        // Given
        final StoreProperties props = new StoreProperties();
        final Set<Class<? extends JSONSerialiserModules>> modules = new HashSet<>();
        modules.add(TestCustomJsonModules1.class);
        modules.add(TestCustomJsonModules2.class);

        // When
        props.setJsonSerialiserModules(modules);

        // Then
        final String expected = TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName();
        assertThat(props.getJsonSerialiserModules()).isEqualTo(expected);
    }

    @Test
    public void shouldGetAndSetAdminAuth() {
        // Given
        final String adminAuth = "admin auth";
        final StoreProperties props = new StoreProperties();

        // When
        props.setAdminAuth(adminAuth);

        // Then
        assertThat(props.getAdminAuth()).isEqualTo(adminAuth);
    }

    @Test
    public void shouldGetAndSetStoreClass() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getStoreClass()).isNull();

        // Given / When
        props.setStoreClass(Store.class);
        // Then
        assertThat(props.getStoreClass()).isEqualTo(Store.class.getName());
    }

    @Test
    public void shouldGetAndSetJobTrackerEnabled() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getJobTrackerEnabled()).isFalse();

        // Given / When
        props.setJobTrackerEnabled(true);
        // Then
        assertThat(props.getJobTrackerEnabled()).isTrue();
    }

    @Test
    public void shouldGetAndSetNamedViewEnabled() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getNamedViewEnabled()).isTrue();

        // Given / When
        props.setNamedViewEnabled(false);
        // Then
        assertThat(props.getNamedViewEnabled()).isFalse();
    }

    @Test
    public void shouldGetAndSetNamedOperationEnabled() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getNamedOperationEnabled()).isTrue();

        // Given / When
        props.setNamedOperationEnabled(false);
        // Then
        assertThat(props.getNamedOperationEnabled()).isFalse();
    }

    @Test
    public void shouldGetAndSetRescheduleJobsOnStart() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getRescheduleJobsOnStart()).isFalse();

        // Given / When
        props.setRescheduleJobsOnStart(true);
        // Then
        assertThat(props.getRescheduleJobsOnStart()).isTrue();
    }

    @Test
    public void shouldGetAndSetSchemaClass() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getSchemaClass()).isEqualTo(Schema.class);

        // Given
        final String invalidSchemaClass = "unchecked.invalid.Schema.class";
        // When
        props.setSchemaClass(invalidSchemaClass);
        // Then
        assertThat(props.getSchemaClassName()).isEqualTo(invalidSchemaClass);

        // Given / When
        props.setSchemaClass(Schema.class);
        // Then
        assertThat(props.getSchemaClass()).isEqualTo(Schema.class);
    }

    @Test
    public void shouldThrowExceptionForInvalidStorePropertiesClass() {
        // Given
        final StoreProperties props = new StoreProperties();
        final String invalidPropsClass = "invalid.props.class";
        // When
        props.setStorePropertiesClassName(invalidPropsClass);
        // Then
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(props::getStorePropertiesClass)
                .withMessage(String.format("Store properties class was not found: %s", invalidPropsClass));
    }

    @Test
    public void shouldThrowExceptionGettingInvalidSchemaClass() {
        // Given
        final StoreProperties props = new StoreProperties();
        final String invalidSchemaClass = "invalid.Schema.class";
        // When
        props.setSchemaClass(invalidSchemaClass);
        // Then
        assertThatExceptionOfType(SchemaException.class)
                .isThrownBy(props::getSchemaClass)
                .withMessage(String.format("Schema class was not found: %s", invalidSchemaClass));
    }

    @Test
    public void shouldGetAndSetJsonSerialiserClass() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getJsonSerialiserClass()).isNull();

        // Given
        String invalidClass = "invalid.class";
        // When
        props.setJsonSerialiserClass(invalidClass);
        // Then
        assertThat(props.getJsonSerialiserClass()).isEqualTo(invalidClass);
    }

    @Test
    public void shouldGetAndSetStrictJson() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getStrictJson()).isNull();

        // Given / When
        props.setStrictJson(true);
        // Then
        assertThat(props.getStrictJson()).isTrue();

        // Given / When
        props.setStrictJson(null);
        // Then
        assertThat(props.getStrictJson()).isNull();
    }

    @Test
    public void shouldGetAndSetDefaultCacheService() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getDefaultCacheServiceClass()).isNull();

        // Given
        final String cacheServiceClassString = "example.cache.class";
        // When
        props.setDefaultCacheServiceClass(cacheServiceClassString);
        // Then
        assertThat(props.getDefaultCacheServiceClass()).isEqualTo(cacheServiceClassString);

        // Given / When
        final StoreProperties props2 = loadStoreProperties("legacyStore.properties");
        // Then
        assertThat(props2.getDefaultCacheServiceClass()).isNotNull();
    }

    @Test
    @Deprecated
    public void shouldGetAndSetCacheServiceWithDeprecatedMethods() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getCacheServiceClass()).isNull();

        // Given / When
        final StoreProperties props2 = loadStoreProperties("legacyStore.properties");
        // Then
        assertThat(props2.getCacheServiceClass()).isNotNull();

        // Given
        final String cacheServiceClassString = "example.cache.class";
        // When
        props.setCacheServiceClass(cacheServiceClassString);
        // Then
        assertThat(props.getCacheServiceClass()).isEqualTo(cacheServiceClassString);
        assertThat(props.getDefaultCacheServiceClass()).isEqualTo(cacheServiceClassString);
    }

    @Test
    public void shouldGetAndSetSpecificCacheServices() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getJobTrackerCacheServiceClass()).isNull();
        assertThat(props.getNamedViewCacheServiceClass()).isNull();
        assertThat(props.getNamedOperationCacheServiceClass()).isNull();

        // Given
        final String cacheServiceClassString = "example.cache.class";
        // When
        props.setJobTrackerCacheServiceClass(cacheServiceClassString);
        props.setNamedViewCacheServiceClass(cacheServiceClassString);
        props.setNamedOperationCacheServiceClass(cacheServiceClassString);
        // Then
        assertThat(props.getJobTrackerCacheServiceClass()).isEqualTo(cacheServiceClassString);
        assertThat(props.getNamedViewCacheServiceClass()).isEqualTo(cacheServiceClassString);
        assertThat(props.getNamedOperationCacheServiceClass()).isEqualTo(cacheServiceClassString);
    }

    @Test
    public void shouldGetAndSetAllowNestedNamedOperation() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.isNestedNamedOperationAllow()).isFalse();
        assertThat(props.isNestedNamedOperationAllow(false)).isFalse();
        assertThat(props.isNestedNamedOperationAllow(true)).isTrue();

        // Given / When
        props.setNestedNamedOperationAllow(true);
        // Then
        assertThat(props.isNestedNamedOperationAllow()).isTrue();
        assertThat(props.isNestedNamedOperationAllow(false)).isTrue();
        assertThat(props.isNestedNamedOperationAllow(true)).isTrue();
    }

    @Test
    public void shouldGetAndSetCacheSuffixes() {
        // Given / When
        final StoreProperties props = new StoreProperties();
        // Then
        assertThat(props.getCacheServiceDefaultSuffix(null)).isNull();
        assertThat(props.getCacheServiceJobTrackerSuffix(null)).isNull();
        assertThat(props.getCacheServiceNamedViewSuffix(null)).isNull();
        assertThat(props.getCacheServiceNamedOperationSuffix(null)).isNull();

        // Given
        final String suffix = "mySuffix";
        // When
        props.setCacheServiceNameSuffix(suffix);
        // Then
        assertThat(props.getCacheServiceDefaultSuffix(null)).isEqualTo(suffix);

        // Given / When
        final StoreProperties props2 = loadStoreProperties("suffixes.properties");
        // Then
        assertThat(props2.getCacheServiceDefaultSuffix(null)).isEqualTo("default");
        assertThat(props2.getCacheServiceJobTrackerSuffix(null)).isEqualTo("jt");
        assertThat(props2.getCacheServiceNamedViewSuffix(null)).isEqualTo("nv");
        assertThat(props2.getCacheServiceNamedOperationSuffix(null)).isEqualTo("no");
    }

    @Test
    public void shouldConstructStorePropertiesFromPath(@TempDir Path tempDir) throws IOException {
        // Given
        Path propsPath = tempDir.resolve("tmp.properties");
        Files.write(propsPath, "key=value".getBytes());
        // When
        StoreProperties props = new StoreProperties(propsPath);
        // Then
        assertThat(props.get("key")).isEqualTo("value");
    }

    @Test
    public void shouldThrowExceptionConstructingStorePropertiesFromMissingPath(@TempDir Path tempDir) {
        // Given
        Path propsPath = tempDir.resolve("missing");
        // When / Then
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> new StoreProperties(propsPath))
                .withMessageContaining("java.nio.file.NoSuchFileException")
                .withMessageContaining("/missing");
    }

    @Test
    public void shouldNotThrowExceptionConstructingStorePropertiesFromNullPath() {
        // Given
        Path propsPath = null;
        // When / Then
        assertThatCode(() -> new StoreProperties(propsPath)).doesNotThrowAnyException();
    }

    @Test
    public void shouldConstructStorePropertiesFromProperties() {
        // Given
        final Properties props = new Properties();
        // When / Then
        assertThatCode(() -> new StoreProperties(props)).doesNotThrowAnyException();
    }

    @Test
    public void shouldReturnStorePropertiesFromPropertiesAndClass() {
        // Given
        final Properties props = new Properties();
        // When / Then
        assertThatCode(() -> loadStoreProperties(props, StoreProperties.class)).doesNotThrowAnyException();
    }

    @Test
    public void shouldThrowExceptionWhenTryingToReturnStorePropertiesFromPropertiesWithInvalidClass() {
        // Given
        final Properties props = new Properties();
        final String className = "invalid.missing.class";
        props.setProperty(StoreProperties.STORE_PROPERTIES_CLASS, className);
        // When / Then
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> loadStoreProperties(props))
                .withMessage(String.format("Failed to create store properties file : %s", className));
    }

    @Test
    public void shouldReturnStorePropertiesFromPathAndClass(@TempDir Path tempDir) throws IOException {
        // Given
        Path propsPath = tempDir.resolve("tmp.properties");
        Files.write(propsPath, "key=value".getBytes());
        // When
        StoreProperties props = loadStoreProperties(propsPath, StoreProperties.class);
        // Then
        assertThat(props.get("key")).isEqualTo("value");
        assertThat(props.getClass()).isEqualTo(StoreProperties.class);
    }

    @Test
    public void shouldReturnStorePropertiesFromPathStringAndClass(@TempDir Path tempDir) throws IOException {
        // Given
        Path propsPath = tempDir.resolve("tmp.properties");
        Files.write(propsPath, "key=value".getBytes());
        String propsPathString = propsPath.toString();
        // When
        StoreProperties props = loadStoreProperties(propsPathString, StoreProperties.class);
        // Then
        assertThat(props.get("key")).isEqualTo("value");
        assertThat(props.getClass()).isEqualTo(StoreProperties.class);
    }

    @Test
    public void shouldReturnStorePropertiesFromInputStreamAndClass() {
        // Given
        InputStream inputStream = StreamUtil.openStream(StorePropertiesTest.class, "store.properties");
        // When
        StoreProperties props = loadStoreProperties(inputStream, StoreProperties.class);
        // Then
        assertThat(props.get("key1")).isEqualTo("value1");
        assertThat(props.getClass()).isEqualTo(StoreProperties.class);
    }

    @Test
    public void shouldReturnStorePropertiesFromNullInputStream() {
        // Given
        InputStream inputStream = null;
        // When / Then
        assertThatCode(() -> loadStoreProperties(inputStream)).doesNotThrowAnyException();
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
