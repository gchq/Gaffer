/*
 * Copyright 2016-2017 Crown Copyright
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
import uk.gov.gchq.gaffer.cache.util.CacheSystemProperty;

import static org.junit.Assert.assertEquals;

public class CacheServiceLoaderTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void before() {
        System.clearProperty(CacheSystemProperty.CACHE_SERVICE_CLASS);
    }

    @Test
    public void shouldLoadHashMapServiceIfNoSystemVariableIsSpecified() {

        // given
        CacheServiceLoader.initialise();

        // when
        ICacheService service = CacheServiceLoader.getService();

        // then
        assert(service instanceof HashMapCacheService);
    }

    @Test
    public void shouldLoadServiceFromSystemVariable() {

        // given
        System.setProperty(CacheSystemProperty.CACHE_SERVICE_CLASS, MockCacheService.class.getName());
        CacheServiceLoader.initialise();

        // when
        ICacheService service = CacheServiceLoader.getService();

        // then
        assert(service instanceof MockCacheService);
    }

    @Test
    public void shouldThrowAnExceptionWhenSystemVariableMisconfigured() {

        // given
        String invalidClassName = "invalid.cache.name";
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(invalidClassName);

        // when
        System.setProperty(CacheSystemProperty.CACHE_SERVICE_CLASS, invalidClassName);
        CacheServiceLoader.initialise();

        // then Exception is thrown
    }

    @Test
    public void shouldUseTheSameServiceAcrossDifferentComponents() {

        // given
        CacheServiceLoader.initialise();

        // when
        ICacheService component1Service = CacheServiceLoader.getService();
        ICacheService component2Service = CacheServiceLoader.getService();

        // then
        assertEquals(component1Service, component2Service);
    }
}
