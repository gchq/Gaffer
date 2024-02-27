/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;

public class FederatedStorePropertiesTest {
    final String key = "gaffer.store.class";
    final String value = FederatedStore.class.getName();
    static Path propertiesFilePath;

    @BeforeAll
    static void init() throws URISyntaxException {
        propertiesFilePath = Paths.get(FederatedStorePropertiesTest.class.getResource("/properties/federatedStore.properties").toURI());
    }

    @Test
    void shouldGetIsPublicAccessAllowed() {
        // Given / When
        final FederatedStoreProperties props = new FederatedStoreProperties();
        // Then
        assertThat(props.getIsPublicAccessAllowed()).isEqualTo("true");
    }

    @Test
    void shouldGetAndSetStoreConfiguredMergeFunctions() {
        // Given / When
        final FederatedStoreProperties props = new FederatedStoreProperties();
        // Then
        assertThat(props.getStoreConfiguredMergeFunctions()).isNull();

        // Given / When
        props.setStoreConfiguredMergeFunctions(FederatedStorePropertiesTest.class.getResource("/integrationTestMergeFunctions.json").toString());
        // Then
        assertThat(props.getStoreConfiguredMergeFunctions()).isNotNull();
    }

    @Test
    void shouldGetCacheServiceFederatedStoreSuffix() {
        // Given / When
        final FederatedStoreProperties props = new FederatedStoreProperties();
        // Then
        assertThat(props.getCacheServiceFederatedStoreSuffix(null)).isNull();
        assertThat(props.getCacheServiceFederatedStoreSuffix("true")).isNotNull();
    }

    @Test
    void shouldGetAndSetFederatedStoreCacheServiceClass() {
        // Given / When
        final FederatedStoreProperties props = new FederatedStoreProperties();
        // Then
        assertThat(props.getFederatedStoreCacheServiceClass()).isNull();
        // Given / When
        props.setFederatedStoreCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        // Then
        assertThat(props.getFederatedStoreCacheServiceClass()).isEqualTo(CACHE_SERVICE_CLASS_STRING);
    }

    @Test
    void shouldReturnFederatedStorePropertiesFromPath() {
        // Given / When
        FederatedStoreProperties props = FederatedStoreProperties.loadStoreProperties(propertiesFilePath);
        // Then
        assertThat(props.get(key)).isEqualTo(value);
        assertThat(props.getClass()).isEqualTo(FederatedStoreProperties.class);
    }

    @Test
    void shouldReturnFederatedStorePropertiesFromPathString() {
        // Given
        String propsPathString = propertiesFilePath.toString();
        // When
        FederatedStoreProperties props = FederatedStoreProperties.loadStoreProperties(propsPathString);
        // Then
        assertThat(props.get(key)).isEqualTo(value);
        assertThat(props.getClass()).isEqualTo(FederatedStoreProperties.class);
    }

    @Test
    void shouldReturnFederatedStorePropertiesFromInputStream() {
        // Given
        InputStream inputStream = StreamUtil.openStream(FederatedStorePropertiesTest.class, "properties/federatedStore.properties");
        // When
        FederatedStoreProperties props = FederatedStoreProperties.loadStoreProperties(inputStream, FederatedStoreProperties.class);
        // Then
        assertThat(props.getClass()).isEqualTo(FederatedStoreProperties.class);
    }
}
