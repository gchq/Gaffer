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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.GetAllNamedViews;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class GetAllNamedViewsHandlerTest {
    public static final String SUFFIX_CACHE_NAME = "suffix";
    private final NamedViewCache namedViewCache = new NamedViewCache(SUFFIX_CACHE_NAME);
    private final AddNamedViewHandler addNamedViewHandler = new AddNamedViewHandler(namedViewCache);
    private final String testNamedViewName = "testNamedViewName";
    private final String testUserId = "testUser";

    private final Context context = new Context(new User.Builder()
            .userId(testUserId)
            .build());

    @Mock
    private Store store;

    private final View view = new View.Builder()
            .edge(TestGroups.EDGE)
            .build();

    private final AddNamedView addNamedView = new AddNamedView.Builder()
            .name(testNamedViewName)
            .view(view)
            .overwrite(false)
            .build();

    private final View view2 = new View.Builder()
            .entity(TestGroups.ENTITY)
            .build();

    private final AddNamedView addNamedView2 = new AddNamedView.Builder()
            .name(testNamedViewName + 2)
            .view(view2)
            .overwrite(false)
            .build();

    private final View viewWithNoAccess = new View.Builder()
            .entity(TestGroups.ENTITY)
            .build();

    private final AddNamedView addNamedViewWithNoAccess = new AddNamedView.Builder()
            .name(testNamedViewName + "WithNoAccess")
            .view(viewWithNoAccess)
            .overwrite(false)
            .readAccessPredicate(new NoAccessPredicate())
            .build();

    @AfterAll
    public static void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldGetAllAccessibleNamedViewsFromCache() throws OperationException, CacheOperationException {
        // Given
        given(store.getProperties()).willReturn(new StoreProperties());
        CacheServiceLoader.initialise("uk.gov.gchq.gaffer.cache.impl.HashMapCacheService");
        namedViewCache.clearCache();

        final NamedViewDetail namedViewAsDetail = new NamedViewDetail.Builder()
                .name(testNamedViewName)
                .view(view)
                .creatorId(context.getUser().getUserId())
                .build();
        addNamedViewHandler.doOperation(addNamedView, context, store);

        final NamedViewDetail namedViewAsDetail2 = new NamedViewDetail.Builder()
                .name(testNamedViewName + 2)
                .view(view2)
                .creatorId(context.getUser().getUserId())
                .build();
        addNamedViewHandler.doOperation(addNamedView2, context, store);
        addNamedViewHandler.doOperation(addNamedViewWithNoAccess, context, store);
        final GetAllNamedViews getAllNamedViews = new GetAllNamedViews.Builder().build();

        // when
        final GetAllNamedViewsHandler getAllNamedViewsHandler = new GetAllNamedViewsHandler(namedViewCache);
        final Iterable<NamedViewDetail> namedViewList = getAllNamedViewsHandler.doOperation(getAllNamedViews, context, store);

        // Then
        assertThat(namedViewList)
                .hasSize(2)
                .contains(namedViewAsDetail)
                .contains(namedViewAsDetail2);
    }
}
