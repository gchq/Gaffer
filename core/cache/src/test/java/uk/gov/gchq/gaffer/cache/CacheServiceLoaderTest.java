/*
 * Copyright 2017-2021 Crown Copyright
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CacheServiceLoaderTest {

    private Properties serviceLoaderProperties = new Properties();

    @BeforeEach
    public void before() {
        serviceLoaderProperties.clear();
    }

    @DisplayName("Should not throw NullPointer when Loader is initialised with null properties")
    @Test
    public void shouldINotThrowNullPointerExceptionOnInitialiseLoader() {
        CacheServiceLoader.initialise(null);
    }

    @Test
    public void shouldLoadServiceFromSystemVariable() {
        serviceLoaderProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, EmptyCacheService.class.getName());
        CacheServiceLoader.initialise(serviceLoaderProperties);

        final ICacheService service = CacheServiceLoader.getService();

        assertThat(service).isInstanceOf(EmptyCacheService.class);
    }

    @Test
    public void shouldThrowAnExceptionWhenSystemVariableIsInvalid() {
        final String invalidClassName = "invalid.cache.name";
        serviceLoaderProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, invalidClassName);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> CacheServiceLoader.initialise(serviceLoaderProperties))
                .withMessage("Failed to instantiate cache using class invalid.cache.name");
    }

    @Test
    public void shouldUseTheSameServiceAcrossDifferentComponents() {
        serviceLoaderProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());
        CacheServiceLoader.initialise(serviceLoaderProperties);

        final ICacheService component1Service = CacheServiceLoader.getService();
        final ICacheService component2Service = CacheServiceLoader.getService();

        assertEquals(component1Service, component2Service);
    }

    @Test
    public void shouldSetServiceToNullAfterCallingShutdown() {
        serviceLoaderProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, EmptyCacheService.class.getName());
        CacheServiceLoader.initialise(serviceLoaderProperties);

        CacheServiceLoader.shutdown();

        assertNull(CacheServiceLoader.getService());
    }
}
