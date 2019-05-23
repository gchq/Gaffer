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

package uk.gov.gchq.gaffer.cache;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class CacheServiceLoaderTest {

    private Properties serviceLoaderProperties = new Properties();
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void before() {
        serviceLoaderProperties.clear();
    }


    @Test
    public void shouldDoNothingOnInitialiseIfNoPropertiesAreGiven() {
        try {
            CacheServiceLoader.initialise(null);
        } catch (final NullPointerException e) {
            fail("Should not have thrown an exception");
        }
    }

    @Test
    public void shouldLoadServiceFromSystemVariable() {

        // given
        serviceLoaderProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, EmptyCacheService.class.getName());
        CacheServiceLoader.initialise(serviceLoaderProperties);

        // when
        ICacheService service = CacheServiceLoader.getService();

        // then
        assert (service instanceof EmptyCacheService);
    }

    @Test
    public void shouldThrowAnExceptionWhenSystemVariableMisconfigured() {

        // given
        String invalidClassName = "invalid.cache.name";
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(invalidClassName);

        // when
        serviceLoaderProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, invalidClassName);
        CacheServiceLoader.initialise(serviceLoaderProperties);

        // then Exception is thrown
    }

    @Test
    public void shouldUseTheSameServiceAcrossDifferentComponents() {
        // given
        serviceLoaderProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());
        CacheServiceLoader.initialise(serviceLoaderProperties);

        // when
        ICacheService component1Service = CacheServiceLoader.getService();
        ICacheService component2Service = CacheServiceLoader.getService();

        // then
        assertEquals(component1Service, component2Service);
    }

    @Test
    public void shouldSetServiceToNullAfterCallingShutdown() {
        // given
        serviceLoaderProperties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, EmptyCacheService.class.getName());
        CacheServiceLoader.initialise(serviceLoaderProperties);

        // when
        CacheServiceLoader.shutdown();

        // then
        assertNull(CacheServiceLoader.getService());
    }
}
