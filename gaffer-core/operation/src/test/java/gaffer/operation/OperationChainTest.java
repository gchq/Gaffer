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

package gaffer.operation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import gaffer.exception.SerialisationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetRelatedEdges;
import org.junit.Test;


public class OperationChainTest {

    @Test
    public void shouldBuildOperationChainWithSingleOperation() throws SerialisationException {
        // Given
        final GetAdjacentEntitySeeds getAdjacentEntitySeeds = mock(GetAdjacentEntitySeeds.class);

        // When
        final OperationChain opChain = new OperationChain.Builder()
                .first(getAdjacentEntitySeeds)
                .build();

        // Then
        assertEquals(1, opChain.getOperations().size());
        assertSame(getAdjacentEntitySeeds, opChain.getOperations().get(0));
    }

    @Test
    public void shouldBuildOperationChain_AdjEntitySeedsThenRelatedEdges() throws SerialisationException {
        // Given
        final GetAdjacentEntitySeeds getAdjacentEntitySeeds = mock(GetAdjacentEntitySeeds.class);
        final GetRelatedEdges<EntitySeed> getRelatedEdges = mock(GetRelatedEdges.class);

        // When
        final OperationChain opChain = new OperationChain.Builder()
                .first(getAdjacentEntitySeeds)
                .then(getRelatedEdges)
                .build();

        // Then
        assertEquals(2, opChain.getOperations().size());
        assertSame(getAdjacentEntitySeeds, opChain.getOperations().get(0));
        assertSame(getRelatedEdges, opChain.getOperations().get(1));
    }
}