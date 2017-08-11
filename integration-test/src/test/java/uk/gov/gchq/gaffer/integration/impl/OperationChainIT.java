/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;

import static org.junit.Assert.assertEquals;

public class OperationChainIT extends AbstractStoreIT {
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldRunOperationChain() throws Exception {
        // Given
        final OperationChain<Iterable<?>> opChain
                = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("A0"))
                        .build())
                .then(new ToVertices.Builder()
                        .edgeVertices(ToVertices.EdgeVertices.BOTH)
                        .build())
                .then(new ToEntitySeeds())
                .then(new ExportToSet.Builder<Iterable<? extends EntitySeed>>()
                        .key("hop1")
                        .build())
                .then(new GetElements())
                .then(new ToVertices.Builder()
                        .edgeVertices(ToVertices.EdgeVertices.BOTH)
                        .build())
                .then(new ToEntitySeeds())
                .then(new ExportToSet.Builder<Iterable<? extends EntitySeed>>()
                        .key("hop2")
                        .build())
                .then(new DiscardOutput())
                .then(new GetSetExport.Builder()
                        .key("hop2")
                        .build())
                .build();

        // When
        final Iterable<?> results = graph.execute(opChain, getUser());

        // Then
        assertEquals(Sets.newHashSet(
                        new EntitySeed("A0"),
                        new EntitySeed("B0"),
                        new EntitySeed("C0"),
                        new EntitySeed("D0")
                ),
                Sets.newHashSet(results));
    }
}