/*
 * Copyright 2017-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandlerTest;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

import java.util.ArrayList;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class FederatedGetAdjacentIdsHandlerTest extends FederatedOperationOutputHandlerTest<GetAdjacentIds, Iterable<? extends EntityId>> {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        o1 = Lists.newArrayList(new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 1)
                .build());
        o2 = Lists.newArrayList(new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 2)
                .build());
        o3 = Lists.newArrayList(new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 3)
                .build());
        o4 = Lists.newArrayList(new Entity.Builder().group(TEST_ENTITY)
                .property(PROPERTY_TYPE, 2)
                .build());
    }

    @Override
    protected FederatedOperationOutputHandler<GetAdjacentIds, Iterable<? extends EntityId>> getFederatedHandler() {
        return new FederatedGetAdjacentIdsHandler();
    }

    @Override
    protected GetAdjacentIds getExampleOperation() {
        return new GetAdjacentIds.Builder().build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected boolean validateMergeResultsFromFieldObjects(final Iterable<? extends EntityId> result,
                                                           final Object... resultParts) {
        assertThat(result).isNotNull();
        final Iterable[] resultPartItrs = Arrays.copyOf(resultParts, resultParts.length, Iterable[].class);
        final ArrayList<Object> elements = Lists.newArrayList(new ChainedIterable<>(resultPartItrs));
        int i = 0;
        for (final EntityId e : result) {
            assertThat(e).isInstanceOf(Entity.class);
            elements.contains(e);
            i++;
        }
        assertThat(elements).hasSize(i);
        return true;
    }
}
