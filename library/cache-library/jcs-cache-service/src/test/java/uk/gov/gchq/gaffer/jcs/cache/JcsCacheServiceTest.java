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

package uk.gov.gchq.gaffer.jcs.cache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.util.CacheSystemProperty;

import java.io.File;

public class JcsCacheServiceTest {

    private JcsCacheService service = new JcsCacheService();
    private static final String TEST_REGION = "test";
    private static final String ALTERNATIVE_TEST_REGION = "alternativeTest";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void before() {
        System.clearProperty(CacheSystemProperty.CACHE_CONFIG_FILE);
    }

    @Test
    public void shouldUseDefaultConfigFileIfNoneIsSpecified() {
        service.initialise();
        ICache<String, Integer> cache = service.getCache(TEST_REGION);
        cache.put("test", 1);
        cache.clear();
        // no exception thrown
    }

    @Test
    public void shouldThrowAnExceptionIfPathIsMisconfigured() {
        String badFileName = "/made/up/file/name";

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(badFileName);

        System.setProperty(CacheSystemProperty.CACHE_CONFIG_FILE, badFileName);

        service.initialise();
    }

    @Test
    public void shouldUseSystemVariableToConfigureJCS() {
        // given
        String filePath = new File("src/test/resources/cache.ccf").getAbsolutePath();
        System.setProperty(CacheSystemProperty.CACHE_CONFIG_FILE, filePath);
        service.initialise();
        // when
        ICache<String, Integer> cache = service.getCache(ALTERNATIVE_TEST_REGION);
        cache.put("test", 1);
        cache.clear();

        // then no exception
    }

    @Test
    public void shouldReUseCacheIfOneExists() {

        // given
        service.initialise();
        ICache<String, Integer> cache = service.getCache(TEST_REGION);
        cache.put("key", 1);

        // when
        ICache<String, Integer> sameCache = service.getCache(TEST_REGION);

        // then
        Assert.assertEquals(1, sameCache.size());
        Assert.assertEquals(new Integer(1), sameCache.get("key"));

        cache.clear();

    }

    @Test
    public void shouldShareCachesBetweenServices() {

        // given
        service.initialise();
        JcsCacheService service1 = new JcsCacheService();
        service1.initialise();

        // when
        ICache<String, Integer> cache = service1.getCache(TEST_REGION);
        cache.put("Test", 2);

        // then
        Assert.assertEquals(1, service.getCache(TEST_REGION).size());
        Assert.assertEquals(2, service.getCache(TEST_REGION).get("Test"));

        cache.clear();

    }
}
