/*
 * Copyright 2017-2021 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;

import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederationOutputIterableHandlerTest;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

public class FederatedGetAdjacentIdsHandlerTest extends FederationOutputIterableHandlerTest<GetAdjacentIds, EntityId> {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        o1 = new WrappedCloseableIterable<>(Lists.newArrayList(
                new Entity.Builder().group(TEST_ENTITY)
                        .property(PROPERTY_TYPE, 1)
                        .build()));
        o2 = new WrappedCloseableIterable<>(Lists.newArrayList(
                new Entity.Builder().group(TEST_ENTITY)
                        .property(PROPERTY_TYPE, 2)
                        .build()));
        o3 = new WrappedCloseableIterable<>(Lists.newArrayList(
                new Entity.Builder().group(TEST_ENTITY)
                        .property(PROPERTY_TYPE, 3)
                        .build()));
        o4 = new WrappedCloseableIterable<>(Lists.newArrayList(
                new Entity.Builder().group(TEST_ENTITY)
                        .property(PROPERTY_TYPE, 2)
                        .build()));
    }

    @Override
    protected FederatedOutputCloseableIterableHandler<GetAdjacentIds, EntityId> getFederationOperationHandler() {
        return new FederatedOutputCloseableIterableHandler<>();
    }

    @Override
    protected GetAdjacentIds getExampleOperation() {
        return new GetAdjacentIds.Builder().build();
    }


}
