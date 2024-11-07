/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.named;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewParameterDetail;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.DeleteNamedView;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class DeleteNamedViewHandlerTest {
    private static final String WRITE_ACCESS_ROLE = "writeRole";
    public static final String SUFFIX = "suffix";
    private final NamedViewCache namedViewCache = new NamedViewCache(SUFFIX);
    private final AddNamedViewHandler addNamedViewHandler = new AddNamedViewHandler(namedViewCache);
    private final DeleteNamedViewHandler deleteNamedViewHandler = new DeleteNamedViewHandler(namedViewCache);
    private final String testNamedViewName = "testNamedViewName";
    private final String invalidNamedViewName = "invalidNamedViewName";
    private final String testUserId = "testUser";
    private final Map<String, ViewParameterDetail> testParameters = new HashMap<>();
    private final Context context = new Context(new User.Builder()
            .userId(testUserId)
            .opAuth(WRITE_ACCESS_ROLE)
            .build());
    private final Store store = mock(Store.class);
    private View view;
    private AddNamedView addNamedView;

    @BeforeEach
    public void before() throws OperationException {
        CacheServiceLoader.shutdown();
        CacheServiceLoader.initialise("uk.gov.gchq.gaffer.cache.impl.HashMapCacheService");

        given(store.getProperties()).willReturn(new StoreProperties());

        testParameters.put("testParam", mock(ViewParameterDetail.class));

        view = new View.Builder()
                .edge(TestGroups.EDGE)
                .build();

        addNamedView = new AddNamedView.Builder()
                .name(testNamedViewName)
                .view(view)
                .writeAccessRoles(WRITE_ACCESS_ROLE)
                .overwrite(false)
                .build();

        addNamedViewHandler.doOperation(addNamedView, context, store);
    }

    @AfterEach
    public void clearCache() throws CacheOperationException {
        namedViewCache.clearCache();
    }

    @AfterAll
    public static void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldDeleteNamedViewCorrectly() throws OperationException, CacheOperationException {
        assertTrue(cacheContains(testNamedViewName));
        // Given

        final DeleteNamedView deleteNamedView = new DeleteNamedView.Builder().name(testNamedViewName).build();

        // When
        deleteNamedViewHandler.doOperation(deleteNamedView, context, store);

        // Then
        assertFalse(cacheContains(testNamedViewName));
    }

    @Test
    public void shouldNotThrowExceptionWhenNoNamedViewToDelete() throws CacheOperationException, OperationException {
        assertTrue(cacheContains(testNamedViewName));

        // Given
        final DeleteNamedView deleteInvalidNamedView = new DeleteNamedView.Builder().name(invalidNamedViewName).build();

        // When
        deleteNamedViewHandler.doOperation(deleteInvalidNamedView, context, store);

        // Then
        assertTrue(cacheContains(testNamedViewName));
    }

    private boolean cacheContains(final String namedViewName) throws CacheOperationException {
        Iterable<NamedViewDetail> namedViews = namedViewCache.getAllNamedViews(context.getUser());
        for (final NamedViewDetail namedView : namedViews) {
            if (namedView.getName().equals(namedViewName)) {
                return true;
            }
        }
        return false;
    }
}
