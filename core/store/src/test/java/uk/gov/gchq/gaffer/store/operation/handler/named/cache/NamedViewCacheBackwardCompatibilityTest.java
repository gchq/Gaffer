/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.named.cache;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.JcsCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.user.User;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamedViewCacheBackwardCompatibilityTest {
    private static NamedViewCache viewCache;
    private static final User ADDING_USER = new User("user1");
    private static final String VIEW_NAME = "TestView";

    @BeforeAll
    public static void setUp() {
        final Properties properties = new Properties();
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, JcsCacheService.class.getName());
        properties.setProperty(CacheProperties.CACHE_CONFIG_FILE, "src/test/resources/gaffer-1.12.0-cache/cache.ccf");
        CacheServiceLoader.initialise(properties);
        viewCache = new NamedViewCache();
    }

    @Test
    public void shouldReturnExpectedNamedViewDetailUsingCacheDataFromVersion1_12() throws Exception {
        final NamedViewDetail namedViewDetail = new NamedViewDetail.Builder()
                .name(VIEW_NAME)
                .description("standard View")
                .creatorId(ADDING_USER.getUserId())
                .writers(asList("writerAuth1", "writerAuth2"))
                .view(new View.Builder().build())
                .build();

        final NamedViewDetail namedViewDetailFromCacheVersion1_12 = viewCache.getNamedView(namedViewDetail.getName());

        assertEquals(namedViewDetail.getOrDefaultReadAccessPredicate(), namedViewDetailFromCacheVersion1_12.getOrDefaultReadAccessPredicate());
        assertEquals(namedViewDetail.getOrDefaultWriteAccessPredicate(), namedViewDetailFromCacheVersion1_12.getOrDefaultWriteAccessPredicate());
    }
}
