/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandlerTest;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.store.operation.handler.compare.SortHandler;
import java.util.Iterator;

public class FederatedSortHandlerTest extends FederatedOperationOutputHandlerTest<Sort, Iterable<? extends Element>> {

    private Sort mockOp;
    private SortHandler mockHandler;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        o1 = new WrappedCloseableIterable<>(Lists.<Element>newArrayList(new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 1)
                .build()));
        o2 = new WrappedCloseableIterable<>(Lists.newArrayList(new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 2)
                .build()));
        o3 = new WrappedCloseableIterable<>(Lists.newArrayList(new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 3)
                .build()));
        o4 = new WrappedCloseableIterable<>(Lists.newArrayList(new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 2)
                .build()));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected FederatedOperationOutputHandler<Sort, Iterable<? extends Element>> getFederatedHandler() {
        mockHandler = mock(SortHandler.class);
        try {
            given(mockHandler.doOperation(eq(mockOp), any(), any()))
                    .willReturn(new WrappedCloseableIterable(Lists.newArrayList(
                            new Entity.Builder().group(TEST_ENTITY)
                                    .property(PROPERTY_TYPE, 1)
                                    .build())
                    ));
        } catch (OperationException e) {
            fail();
        }
        return new FederatedSortHandler(mockHandler);
    }

    @Override
    public Sort getExampleOperation() {
        mockOp = mock(Sort.class);
        return mockOp;
    }

    @Override
    protected boolean validateMergeResultsFromFieldObjects(final Iterable<? extends Element> result) {
        try {
            verify(mockHandler).doOperation(eq(mockOp), any(), any());
        } catch (OperationException e) {
            fail();
        }
        Assert.assertNotNull("Result is null", result);
        CloseableIterator<Element> expectedIt = new ChainedIterable<Element>(o1, o2, o3, o4).iterator();
        Iterator<? extends Element> resultIt = result.iterator();
        Assert.assertEquals(expectedIt.next(), resultIt.next());
        Assert.assertFalse(resultIt.hasNext());

        return true;
    }

    @Test
    public void shouldNotThrowExceptionWhenDefaultConstructorIsCalled() throws Exception {
        Assert.assertNotNull(new FederatedSortHandler());
    }
}