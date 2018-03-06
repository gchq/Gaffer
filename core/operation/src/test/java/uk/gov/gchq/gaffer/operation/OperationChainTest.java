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
import com.google.common.collect.Lists;
import org.junit.Test;

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
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.io.Output;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class OperationChainTest extends OperationsTest<OperationChain> {
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
    public void shouldBuildOperationChainWithTypeUnsafe() {
        // When
        final GetAdjacentIds getAdjIds1 = new GetAdjacentIds();
        final ExportToSet<CloseableIterable<? extends EntityId>> exportToSet1 = new ExportToSet<>();
        final DiscardOutput discardOutput1 = new DiscardOutput();
        final GetSetExport getSetExport1 = new GetSetExport();
        final GetAdjacentIds getAdjIds2 = new GetAdjacentIds();
        final ExportToSet<CloseableIterable<? extends EntityId>> exportToSet2 = new ExportToSet<>();
        final DiscardOutput discardOutput2 = new DiscardOutput();
        final GetSetExport getSetExport2 = new GetSetExport();
        final OperationChain<CloseableIterable<? extends EntityId>> opChain = new Builder()
                .first(getAdjIds1)
                .then(exportToSet1)
                .then(discardOutput1)
                .then(getSetExport1)
                .thenTypeUnsafe(getAdjIds2)  // we can use the type unsafe here as we know the output from the set export will be an Iterable of EntityIds
                .then(exportToSet2)
                .then(discardOutput2)
                .then(getSetExport2)
                .buildTypeUnsafe(); // again we can use the type unsafe here as we know the output from the set export will be an Iterable of EntityIds

        // Then
        assertArrayEquals(new Operation[]{
                        getAdjIds1,
                        exportToSet1,
                        discardOutput1,
                        getSetExport1,
                        getAdjIds2,
                        exportToSet2,
                        discardOutput2,
                        getSetExport2
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
        final Map<String, String> options = mock(Map.class);
        opChain.setOptions(options);

        // When
        final OperationChain clone = opChain.shallowClone();

        // Then
        assertEquals(clonedOps, clone.getOperations());
        assertSame(options, clone.getOptions());
    }

    @Test
    public void shouldWrapOperation() throws IOException {
        // Given
        final Operation operation = mock(Operation.class);
        final Map<String, String> options = mock(Map.class);
        given(operation.getOptions()).willReturn(options);

        // When
        final OperationChain wrappedChain = OperationChain.wrap(operation);

        // Then
        assertEquals(1, wrappedChain.getOperations().size());
        assertEquals(operation, wrappedChain.getOperations().get(0));
        assertSame(operation.getOptions(), wrappedChain.getOptions());
    }

    @Test
    public void shouldWrapOutputOperation() throws IOException {
        // Given
        final Operation operation = mock(Output.class);
        final Map<String, String> options = mock(Map.class);
        given(operation.getOptions()).willReturn(options);

        // When
        final OperationChain wrappedChain = OperationChain.wrap(operation);

        // Then
        assertEquals(1, wrappedChain.getOperations().size());
        assertEquals(operation, wrappedChain.getOperations().get(0));
        assertSame(operation.getOptions(), wrappedChain.getOptions());
    }

    @Test
    public void shouldNotWrapOperationChain() throws IOException {
        // Given
        final Operation operation = mock(OperationChain.class);
        final Map<String, String> options = mock(Map.class);
        given(operation.getOptions()).willReturn(options);

        // When
        final OperationChain wrappedChain = OperationChain.wrap(operation);

        // Then
        assertSame(operation, wrappedChain);
        assertSame(operation.getOptions(), wrappedChain.getOptions());
    }

    @Test
    public void shouldNotWrapOperationChainDAO() throws IOException {
        // Given
        final Operation operation = mock(OperationChainDAO.class);
        final Map<String, String> options = mock(Map.class);
        given(operation.getOptions()).willReturn(options);

        // When
        final OperationChain wrappedChain = OperationChain.wrap(operation);

        // Then
        assertSame(operation, wrappedChain);
        assertSame(operation.getOptions(), wrappedChain.getOptions());
    }


    @Override
    protected OperationChain getTestObject() {
        return new OperationChain();
    }

    @Override
    public void shouldGetOperations() {
        // Given
        final List<Operation> ops = Lists.newArrayList(
                mock(Operation.class),
                mock(GetAllElements.class),
                mock(Aggregate.class),
                mock(Limit.class)
        );

        final OperationChain<Operation> opChain = new OperationChain<>(ops);

        // When
        final Collection<Operation> getOps = opChain.getOperations();

        // Then
        assertEquals(ops, getOps);
    }
}
