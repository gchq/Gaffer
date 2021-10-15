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
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamedOperationCacheBackwardCompatibilityTest {

    private static NamedOperationCache operationCache;
    private static final User ADDING_USER = new User("user1");
    private static final String OPERATION_NAME = "TestOperation";

    @BeforeAll
    public static void setUp() {
        final Properties properties = new Properties();
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, JcsCacheService.class.getName());
        properties.setProperty(CacheProperties.CACHE_CONFIG_FILE, "src/test/resources/gaffer-1.12.0-cache/cache.ccf");
        CacheServiceLoader.initialise(properties);
        operationCache = new NamedOperationCache();
    }

    @Test
    public void shouldReturnExpectedNamedOperationDetailUsingCacheDataFromVersion1_12() throws Exception {
        final NamedOperationDetail namedOperationDetail = new NamedOperationDetail.Builder()
                .operationName(OPERATION_NAME)
                .description("standard operation")
                .creatorId(ADDING_USER.getUserId())
                .readers(asList("readerAuth1", "readerAuth2"))
                .writers(asList("writerAuth1", "writerAuth2"))
                .operationChain(new OperationChain.Builder().first(new AddElements()).build())
                .build();

        final NamedOperationDetail namedOperationDetailFromCacheVersion1_12 = operationCache.getNamedOperation(OPERATION_NAME, ADDING_USER);

        assertEquals(namedOperationDetail.getReadAccessPredicate(), namedOperationDetailFromCacheVersion1_12.getReadAccessPredicate());
        assertEquals(namedOperationDetail.getWriteAccessPredicate(), namedOperationDetailFromCacheVersion1_12.getWriteAccessPredicate());
    }
}
