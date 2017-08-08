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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;

import org.junit.Assert;
import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.store.operation.handler.compare.MinHandler;

public class FederatedMinHandlerTest extends FederatedItElementToElementHandlerTest<Min, MinHandler> {

    @Override
    protected FederatedOperationOutputHandler<Min, Element> getFederatedHandler() {
        mockHandler = mock(MinHandler.class);
        try {
            given(mockHandler.doOperation(eq(mockOp), any(), any())).willReturn(o1);
        } catch (OperationException e) {
            fail();
        }
        return new FederatedMinHandler(mockHandler);
    }

    @Override
    protected Min getExampleOperation() {
        mockOp = mock(Min.class);
        return mockOp;
    }

    @Test
    public void shouldNotThrowExceptionWhenDefaultConstructorIsCalled() throws Exception {
        Assert.assertNotNull(new FederatedMinHandler());
    }
}