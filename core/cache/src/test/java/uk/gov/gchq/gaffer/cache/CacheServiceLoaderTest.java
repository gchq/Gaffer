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

package uk.gov.gchq.gaffer.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class CacheServiceLoaderTest {
    private final Properties emptyProperties = new Properties();
    private final String emptyCacheServiceName = "emptyService";
    private final String emptyCacheServiceClass = EmptyCacheService.class.getName();

    @BeforeEach
    void reset() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldInitialiseWithNameClassAndProperties() {
        // When
        CacheServiceLoader.initialise(emptyCacheServiceName, emptyCacheServiceClass, emptyProperties);

        // Then
        assertThat(CacheServiceLoader.getService(emptyCacheServiceName)).isNotNull();
    }

    @Test
    void shouldInitialiseWithOnlyClass() {
        // When
        CacheServiceLoader.initialise(emptyCacheServiceClass);

        // Then
        assertThat(CacheServiceLoader.getDefaultService()).isNotNull();
    }

    @Test
    void shouldInitialiseOnceOnlyWithSameServiceName() {
        // Given
        CacheServiceLoader.initialise(emptyCacheServiceClass);
        final ICacheService initialService = CacheServiceLoader.getDefaultService();

        // When
        // Should have no effect as the default service was already initialized
        CacheServiceLoader.initialise(emptyCacheServiceClass);

        // Then
        // Should reference exactly the same object, proving it was not overwritten
        assertThat(CacheServiceLoader.getDefaultService()).isSameAs(initialService);
    }

    @Test
    void shouldThrowExceptionWhenInitialisingWithNullClass() {
        // When / Then
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> CacheServiceLoader.initialise(emptyCacheServiceName, null, emptyProperties))
                .withMessage("Failed to instantiate cache, cache class was null/missing");
    }

    @Test
    void shouldThrowExceptionWhenInitialisingWithNullName() {
        // When / Then
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> CacheServiceLoader.initialise(null, emptyCacheServiceClass, emptyProperties))
                .withMessage("Failed to instantiate cache, service name was null/missing");
    }

    @Test
    void shouldThrowExceptionWhenInitialisingWithMissingClass() {
        // When / Then
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> CacheServiceLoader.initialise(emptyCacheServiceName, "a.class.which.does.not.exist", emptyProperties))
                .withMessage("Failed to instantiate cache, class 'a.class.which.does.not.exist' is missing or invalid");
    }

    @Test
    void shouldThrowExceptionWhenInitialisingWithInvalidClass() {
        // When / Then
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> CacheServiceLoader.initialise(emptyCacheServiceName, ICacheService.class.getName(), emptyProperties))
                .withMessage("Failed to instantiate cache using class uk.gov.gchq.gaffer.cache.ICacheService");
    }

    @Test
    void shouldGetServiceEnabledBooleans() {
        // Given
        // Nothing initialised

        // When / Then
        assertThat(CacheServiceLoader.isDefaultEnabled()).isFalse();
        assertThat(CacheServiceLoader.isEnabled(emptyCacheServiceName)).isFalse();

        // Given
        // Initialise empty & default services
        CacheServiceLoader.initialise(emptyCacheServiceName, emptyCacheServiceClass, emptyProperties);
        CacheServiceLoader.initialise(emptyCacheServiceClass);

        // When / Then
        assertThat(CacheServiceLoader.isDefaultEnabled()).isTrue();
        assertThat(CacheServiceLoader.isEnabled(emptyCacheServiceName)).isTrue();
    }

    @Test
    void shouldGetServices() {
        // Given
        // Nothing initialised

        // When / Then
        assertThat(CacheServiceLoader.getDefaultService()).isNull();
        assertThat(CacheServiceLoader.getService(emptyCacheServiceName)).isNull();

        // Given
        // Initialise empty & default services
        CacheServiceLoader.initialise(emptyCacheServiceName, emptyCacheServiceClass, emptyProperties);
        CacheServiceLoader.initialise(emptyCacheServiceClass);

        // When / Then
        assertThat(CacheServiceLoader.getDefaultService()).isInstanceOf(EmptyCacheService.class).isNotNull();
        assertThat(CacheServiceLoader.getService(emptyCacheServiceName)).isInstanceOf(EmptyCacheService.class).isNotNull();
        assertThat(CacheServiceLoader.getDefaultService()).isNotSameAs(CacheServiceLoader.getService(emptyCacheServiceName));
    }
}
