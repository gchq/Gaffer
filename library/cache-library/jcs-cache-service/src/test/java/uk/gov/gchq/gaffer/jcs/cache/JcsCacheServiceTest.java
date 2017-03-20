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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.util.CacheSystemProperty;

import java.io.File;

public class JcsCacheServiceTest {

    private JcsCacheService service = new JcsCacheService();
    private static final String CACHE_NAME = "test";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void before() {
        System.clearProperty(CacheSystemProperty.CACHE_CONFIG_FILE);
    }

    @Test
    public void shouldUseDefaultConfigFileIfNoneIsSpecified() {
        service.initialise();
        ICache<String, Integer> cache = service.getCache(CACHE_NAME);
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
        String filePath = new File("src/test/resources/cache.ccf").getAbsolutePath();

        System.setProperty(CacheSystemProperty.CACHE_CONFIG_FILE, filePath);

        service.initialise();
        ICache<String, Integer> cache = service.getCache("alternativeTest");
        cache.put("test", 1);
        cache.clear();
    }
}
