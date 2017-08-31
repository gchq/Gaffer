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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.OperationImpl;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.MultiInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class OperationChainTest extends JSONSerialisationTest<OperationChain> {
    @Test
    public void shouldSerialiseAndDeserialiseOperationChain() throws SerialisationException {
        // Given
        final OperationChain opChain = new Builder()
                .first(new OperationImpl())
                .then(new OperationImpl())
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(opChain, true);
        final OperationChain deserialisedOp = JSONSerialiser.deserialise(json, OperationChain.class);

        // Then
        assertNotNull(deserialisedOp);
        assertEquals(2, deserialisedOp.getOperations().size());
        assertEquals(OperationImpl.class, deserialisedOp.getOperations()
                                                        .get(0)
                                                        .getClass());
        assertEquals(OperationImpl.class, deserialisedOp.getOperations()
                                                        .get(1)
                                                        .getClass());
    }

    @Test
    public void shouldBuildOperationChain() {
        // Given
        final AddElements addElements1 = mock(AddElements.class);
        final AddElements addElements2 = mock(AddElements.class);
        final GetAdjacentIds getAdj1 = mock(GetAdjacentIds.class);
        final GetAdjacentIds getAdj2 = mock(GetAdjacentIds.class);
        final GetAdjacentIds getAdj3 = mock(GetAdjacentIds.class);
        final GetElements getElements1 = mock(GetElements.class);
        final GetElements getElements2 = mock(GetElements.class);
        final GetAllElements getAllElements = mock(GetAllElements.class);
        final DiscardOutput discardOutput = mock(DiscardOutput.class);
        final GetJobDetails getJobDetails = mock(GetJobDetails.class);
        final GenerateObjects<EntityId> generateEntitySeeds = mock(GenerateObjects.class);
        final Limit<Element> limit = mock(Limit.class);
        final ToSet<Element> deduplicate = mock(ToSet.class);
        final CountGroups countGroups = mock(CountGroups.class);
        final ExportToSet<GroupCounts> exportToSet = mock(ExportToSet.class);
        final ExportToGafferResultCache<CloseableIterable<? extends Element>> exportToGafferCache = mock(ExportToGafferResultCache.class);

        // When
        final OperationChain<JobDetail> opChain = new Builder()
                .first(addElements1)
                .then(getAdj1)
                .then(getAdj2)
                .then(getElements1)
                .then(generateEntitySeeds)
                .then(getAdj3)
                .then(getElements2)
                .then(deduplicate)
                .then(limit)
                .then(countGroups)
                .then(exportToSet)
                .then(discardOutput)
                .then(getAllElements)
                .then(exportToGafferCache)
                .then(addElements2)
                .then(getJobDetails)
                .build();

        // Then
        assertArrayEquals(new Operation[]{
                        addElements1,
                        getAdj1,
                        getAdj2,
                        getElements1,
                        generateEntitySeeds,
                        getAdj3,
                        getElements2,
                        deduplicate,
                        limit,
                        countGroups,
                        exportToSet,
                        discardOutput,
                        getAllElements,
                        exportToGafferCache,
                        addElements2,
                        getJobDetails
                },
                opChain.getOperationArray());
    }

    @Test
    public void shouldBuildOperationChainWithSingleOperation() throws SerialisationException {
        // Given
        final GetAdjacentIds getAdjacentIds = mock(GetAdjacentIds.class);

        // When
        final OperationChain opChain = new OperationChain.Builder()
                .first(getAdjacentIds)
                .build();

        // Then
        assertEquals(1, opChain.getOperations().size());
        assertSame(getAdjacentIds, opChain.getOperations().get(0));
    }

    @Test
    public void shouldBuildOperationChain_AdjEntitySeedsThenElements() throws SerialisationException {
        // Given
        final GetAdjacentIds getAdjacentIds = mock(GetAdjacentIds.class);
        final GetElements getEdges = mock(GetElements.class);

        // When
        final OperationChain opChain = new OperationChain.Builder()
                .first(getAdjacentIds)
                .then(getEdges)
                .build();

        // Then
        assertEquals(2, opChain.getOperations().size());
        assertSame(getAdjacentIds, opChain.getOperations().get(0));
        assertSame(getEdges, opChain.getOperations().get(1));
    }

    @Test
    public void shouldDetermineOperationChainOutputType() {
        // Given
        final Operation operation1 = mock(Operation.class);
        final GetElements operation2 = mock(GetElements.class);
        final TypeReference typeRef = mock(TypeReference.class);

        given(operation2.getOutputTypeReference()).willReturn(typeRef);

        // When
        final OperationChain opChain = new OperationChain.Builder()
                .first(operation1)
                .then(operation2)
                .build();

        // When / Then
        assertSame(typeRef, opChain.getOutputTypeReference());
    }

    @Test
    public void shouldCloseAllOperationInputs() throws IOException {
        // Given
        final Operation[] operations = {
                mock(Operation.class),
                mock(Input.class),
                mock(Input.class),
                mock(MultiInput.class),
                mock(Input.class)
        };

        // When
        final OperationChain opChain = new OperationChain(Arrays.asList(operations));

        // When
        opChain.close();

        // Then
        for (final Operation operation : operations) {
            verify(operation).close();
        }
    }

    @Test
    public void shouldFlattenNestedOperationChain() {
        // Given
        final AddElements addElements = mock(AddElements.class);
        final GetElements getElements = mock(GetElements.class);
        final Limit<Element> limit = mock(Limit.class);

        final OperationChain opChain1 = new OperationChain.Builder().first(addElements)
                                                                    .then(getElements)
                                                                    .build();

        final OperationChain<?> opChain2 = new OperationChain.Builder().first(opChain1)
                                                                       .then(limit)
                                                                       .build();
        // When
        final List<Operation> operations = opChain2.flatten();

        // Then
        final Operation first = operations.get(0);
        final Operation second = operations.get(1);
        final Operation third = operations.get(2);

        assertThat(first, instanceOf(AddElements.class));
        assertThat(second, instanceOf(GetElements.class));
        assertThat(third, instanceOf(Limit.class));
    }

    @Test
    public void shouldDoAShallowClone() throws IOException {
        // Given
        final List<Operation> ops = Arrays.asList(
                mock(Operation.class),
                mock(Input.class),
                mock(Input.class),
                mock(MultiInput.class),
                mock(Input.class)
        );
        final List<Operation> clonedOps = Arrays.asList(
                mock(Operation.class),
                mock(Input.class),
                mock(Input.class),
                mock(MultiInput.class),
                mock(Input.class)
        );
        for (int i = 0; i < ops.size(); i++) {
            given(ops.get(i).shallowClone()).willReturn(clonedOps.get(i));
        }

        final OperationChain opChain = new OperationChain(ops);

        // When
        final OperationChain clone = opChain.shallowClone();

        // Then
        assertEquals(clonedOps, clone.getOperations());
    }

    @Override
    protected OperationChain getTestObject() {
        return new OperationChain();
    }

}
