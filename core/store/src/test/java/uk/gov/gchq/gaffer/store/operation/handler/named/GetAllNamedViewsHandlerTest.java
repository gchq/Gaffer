/*
 * Copyright 2017-2018 Crown Copyright
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

import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetAllNamedViewsHandlerTest {
    private final NamedViewCache namedViewCache = new NamedViewCache();
    private final AddNamedViewHandler addNamedViewHandler = new AddNamedViewHandler(namedViewCache);
    private final String testNamedViewName = "testNamedViewName";
    private final String testUserId = "testUser";

    private Context context = new Context(new User.Builder()
            .userId(testUserId)
            .build());

    private Store store = mock(Store.class);

    private View view = new View.Builder()
            .edge(TestGroups.EDGE)
            .build();

    private AddNamedView addNamedView = new AddNamedView.Builder()
            .name(testNamedViewName)
            .view(view)
            .overwrite(false)
            .build();

    private View view2 = new View.Builder()
            .entity(TestGroups.ENTITY)
            .build();

    private AddNamedView addNamedView2 = new AddNamedView.Builder()
            .name(testNamedViewName + 2)
            .view(view2)
            .overwrite(false)
            .build();

    @AfterClass
    public static void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldGetAllNamedViewsFromCache() throws OperationException {
        // Given
        given(store.getProperties()).willReturn(new StoreProperties());
        StoreProperties properties = new StoreProperties();
        properties.set("gaffer.cache.service.class", "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService");
        CacheServiceLoader.initialise(properties.getProperties());
        NamedViewDetail namedViewAsDetail = new NamedViewDetail.Builder()
                .name(testNamedViewName)
                .view(view)
                .creatorId(context.getUser().getUserId())
                .build();
        NamedViewDetail namedViewAsDetail2 = new NamedViewDetail.Builder()
                .name(testNamedViewName + 2)
                .view(view2)
                .creatorId(context.getUser().getUserId())
                .build();
        addNamedViewHandler.doOperation(addNamedView, context, store);
        addNamedViewHandler.doOperation(addNamedView2, context, store);
        GetAllNamedViews getAllNamedViews = new GetAllNamedViews.Builder().build();

        // when
        GetAllNamedViewsHandler getAllNamedViewsHandler = new GetAllNamedViewsHandler(namedViewCache);
        CloseableIterable<NamedViewDetail> namedViewList = getAllNamedViewsHandler.doOperation(getAllNamedViews, context, store);

        // Then
        assertEquals(2, Iterables.size(namedViewList));
        assertTrue(Iterables.contains(namedViewList, namedViewAsDetail));
        assertTrue(Iterables.contains(namedViewList, namedViewAsDetail2));
    }
}
