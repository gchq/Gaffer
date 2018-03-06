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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.Before;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandlerTest;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

public abstract class FederatedItElementToElementHandlerTest<
        OP extends InputOutput<Iterable<? extends Element>, Element>,
        OPH extends OutputOperationHandler<OP, Element>>
        extends FederatedOperationOutputHandlerTest<OP, Element> {

    protected OP mockOp;
    protected OPH mockHandler;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        o1 = new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 1)
                .build();
        o2 = new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 2)
                .build();
        o3 = new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 3)
                .build();
        o4 = new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 2)
                .build();
    }

    @Override
    protected boolean validateMergeResultsFromFieldObjects(final Element result, final Object... resultParts) {
        assertNotNull(result);
        assertTrue(result instanceof Entity);
        assertEquals(1, result.getProperty(PROPERTY_TYPE));
        try {
            verify(mockHandler).doOperation(eq(mockOp), any(), any());
        } catch (OperationException e) {
            fail();
        }
        verify(mockOp).setInput(Arrays.asList(o1, o2, o3, o4));
        return true;
    }
}
