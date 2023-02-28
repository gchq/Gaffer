/*
 * Copyright 2016-2020 Crown Copyright
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.data.GroupCounts;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.If;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class OperationChainTest extends OperationsTest<OperationChain<?>> {

    @Test
    public void shouldSerialiseAndDeserialiseOperationChain() throws SerialisationException {
        // Given
        final OperationChain<?> opChain = new Builder()
                .first(new OperationImpl())
                .then(new OperationImpl())
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(opChain, true);
        final OperationChain<?> deserialisedOp = JSONSerialiser.deserialise(json, OperationChain.class);

        // Then
        assertThat(deserialisedOp).isNotNull();
        assertThat(deserialisedOp.getOperations()).hasSize(2);
        assertThat(deserialisedOp.getOperations().get(0)).isInstanceOf(OperationImpl.class);
        assertThat(deserialisedOp.getOperations().get(1)).isInstanceOf(OperationImpl.class);
    }

    @Test
    public void shouldBuildOperationChain(@Mock final AddElements addElements1, @Mock final AddElements addElements2,
                                          @Mock final GetAdjacentIds getAdj1, @Mock final GetAdjacentIds getAdj2,
                                          @Mock final GetAdjacentIds getAdj3,
                                          @Mock final GetElements getElements1, @Mock final GetElements getElements2,
                                          @Mock final GetAllElements getAllElements,
                                          @Mock final DiscardOutput discardOutput,
                                          @Mock final GetJobDetails getJobDetails,
                                          @Mock final GenerateObjects<EntityId> generateEntitySeeds,
                                          @Mock final Limit<Element> limit,
                                          @Mock final ToSet<Element> deduplicate,
                                          @Mock final CountGroups countGroups,
                                          @Mock final ExportToSet<GroupCounts> exportToSet,
                                          @Mock final ExportToGafferResultCache<Iterable<? extends Element>> exportToGafferCache,
                                          @Mock final If<Iterable<? extends EntityId>, Iterable<? extends EntityId>> ifOp) {
        // Given

        // When
        final OperationChain<JobDetail> opChain = new Builder()
                .first(addElements1)
                .then(getAdj1)
                .then(getAdj2)
                .then(getElements1)
                .then(generateEntitySeeds)
                .then(getAdj3)
                .then(ifOp)
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
        final Operation[] expecteds = {
                addElements1,
                getAdj1,
                getAdj2,
                getElements1,
                generateEntitySeeds,
                getAdj3,
                ifOp,
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
        };
        assertThat(opChain.getOperationArray()).isEqualTo(expecteds);
    }

    @Test
    public void shouldBuildOperationChainWithTypeUnsafe() {
        // When
        final GetAdjacentIds getAdjIds1 = new GetAdjacentIds();
        final ExportToSet<Iterable<? extends EntityId>> exportToSet1 = new ExportToSet<>();
        final DiscardOutput discardOutput1 = new DiscardOutput();
        final GetSetExport getSetExport1 = new GetSetExport();
        final GetAdjacentIds getAdjIds2 = new GetAdjacentIds();
        final ExportToSet<Iterable<? extends EntityId>> exportToSet2 = new ExportToSet<>();
        final DiscardOutput discardOutput2 = new DiscardOutput();
        final GetSetExport getSetExport2 = new GetSetExport();
        final OperationChain<Iterable<? extends EntityId>> opChain = new Builder()
                .first(getAdjIds1)
                .then(exportToSet1)
                .then(discardOutput1)
                .then(getSetExport1)
                .thenTypeUnsafe(getAdjIds2) // we can use the type unsafe here as we know the output from the set export will be an Iterable of EntityIds
                .then(exportToSet2)
                .then(discardOutput2)
                .then(getSetExport2)
                .buildTypeUnsafe(); // again we can use the type unsafe here as we know the output from the set export will be an Iterable of EntityIds

        // Then
        final Operation[] expecteds = {
                getAdjIds1,
                exportToSet1,
                discardOutput1,
                getSetExport1,
                getAdjIds2,
                exportToSet2,
                discardOutput2,
                getSetExport2
        };
        assertThat(opChain.getOperationArray()).isEqualTo(expecteds);
    }

    @Test
    public void shouldBuildOperationChainWithSingleOperation(@Mock final GetAdjacentIds getAdjacentIds) {
        // Given

        // When
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(getAdjacentIds)
                .build();

        // Then
        assertThat(opChain.getOperations()).hasSize(1);
        assertThat(opChain.getOperations().get(0)).isSameAs(getAdjacentIds);
    }

    @Test
    public void shouldBuildOperationChain_AdjEntitySeedsThenElements(@Mock final GetAdjacentIds getAdjacentIds,
                                                                     @Mock final GetElements getEdges) {
        // Given

        // When
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(getAdjacentIds)
                .then(getEdges)
                .build();

        // Then
        assertThat(opChain.getOperations()).hasSize(2);
        assertThat(opChain.getOperations().get(0)).isSameAs(getAdjacentIds);
        assertThat(opChain.getOperations().get(1)).isSameAs(getEdges);
    }

    @Test
    public void shouldDetermineOperationChainOutputType(@Mock final Operation operation1,
                                                        @Mock final GetElements operation2,
                                                        @Mock final TypeReference typeRef) {
        // Given

        given(operation2.getOutputTypeReference()).willReturn(typeRef);

        // When
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(operation1)
                .then(operation2)
                .build();

        // When / Then
        assertThat(opChain.getOutputTypeReference()).isSameAs(typeRef);
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
        final OperationChain<?> opChain = new OperationChain<>(Arrays.asList(operations));

        // When
        opChain.close();

        // Then
        for (final Operation operation : operations) {
            verify(operation).close();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldFlattenNestedOperationChain(@Mock final AddElements addElements,
                                                  @Mock final GetElements getElements,
                                                  @Mock final Limit<Element> limit) {
        // Given
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

        assertThat(first).isInstanceOf(AddElements.class);
        assertThat(second).isInstanceOf(GetElements.class);
        assertThat(third).isInstanceOf(Limit.class);
    }

    @Test
    public void shouldDoAShallowClone(@Mock final Map<String, String> options) {
        // Given
        final List<Operation> ops = Arrays.asList(
                mock(Operation.class),
                mock(Input.class),
                mock(Input.class),
                mock(MultiInput.class),
                mock(Input.class));
        final List<Operation> clonedOps = Arrays.asList(
                mock(Operation.class),
                mock(Input.class),
                mock(Input.class),
                mock(MultiInput.class),
                mock(Input.class));
        for (int i = 0; i < ops.size(); i++) {
            given(ops.get(i).shallowClone()).willReturn(clonedOps.get(i));
        }

        OperationChain<?> opChain = null;
        try {
            opChain = new OperationChain<>(ops);
            opChain.setOptions(options);

            // When
            final OperationChain<?> clone = opChain.shallowClone();

            // Then
            assertThat(clone.getOperations()).isEqualTo(clonedOps);
            assertThat(clone.getOptions()).isSameAs(options);
        } finally {
            CloseableUtil.close(opChain);
        }
    }

    @Test
    public void shouldWrapOperation(@Mock final Operation operation,
                                    @Mock final Map<String, String> options) {
        // Given
        given(operation.getOptions()).willReturn(options);

        // When
        final OperationChain<?> wrappedChain = OperationChain.wrap(operation);

        // Then
        assertThat(wrappedChain.getOperations()).hasSize(1);
        assertThat(wrappedChain.getOperations().get(0)).isEqualTo(operation);
        assertThat(wrappedChain.getOptions()).isSameAs(operation.getOptions());
    }

    @Test
    public void shouldWrapOutputOperation(@Mock final Operation operation,
                                          @Mock final Map<String, String> options) {
        // Given
        given(operation.getOptions()).willReturn(options);

        // When
        final OperationChain<?> wrappedChain = OperationChain.wrap(operation);

        // Then
        assertThat(wrappedChain.getOperations()).hasSize(1);
        assertThat(wrappedChain.getOperations().get(0)).isEqualTo(operation);
        assertThat(wrappedChain.getOptions()).isSameAs(operation.getOptions());
    }

    @Test
    public void shouldNotWrapOperationChain(@Mock final OperationChain operation,
                                            @Mock final Map<String, String> options) {
        // Given
        given(operation.getOptions()).willReturn(options);

        // When
        final OperationChain<?> wrappedChain = OperationChain.wrap(operation);

        // Then
        assertThat(wrappedChain).isSameAs(operation);
        assertThat(wrappedChain.getOptions()).isSameAs(operation.getOptions());
    }

    @Test
    public void shouldNotWrapOperationChainDAO(@Mock final OperationChainDAO<?> operation,
                                               @Mock final Map<String, String> options) {
        // Given
        given(operation.getOptions()).willReturn(options);

        // When
        final OperationChain<?> wrappedChain = OperationChain.wrap(operation);

        // Then
        assertThat(wrappedChain).isSameAs(operation);
        assertThat(wrappedChain.getOptions()).isSameAs(operation.getOptions());
    }

    @Override
    protected OperationChain<?> getTestObjectOld() {
        return new OperationChain<>();
    }

    @Test
    @Override
    public void shouldGetOperations() {
        // Given
        final List<Operation> ops = Lists.newArrayList(mock(Operation.class),
                mock(GetAllElements.class),
                mock(Aggregate.class),
                mock(Limit.class));
        OperationChain<Operation> opChain = null;
        try {
            opChain = new OperationChain<>(ops);

            // When
            final Collection<Operation> getOps = opChain.getOperations();

            // Then
            assertThat(getOps).isEqualTo(ops);
        } finally {
            CloseableUtil.close(opChain);
        }
    }

    @Test
    public void shouldConvertToOverviewString() {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds.Builder()
                        .input(new EntitySeed("vertex1"))
                        .build())
                .then(new Limit<>(1))
                .build();

        // When
        final String overview = opChain.toOverviewString();

        // Then
        assertThat(overview).isEqualTo("OperationChain[GetAdjacentIds->Limit]");
    }

    @Test
    public void shouldConvertToOverviewStringWithNoOperations() {
        OperationChain<?> opChain = null;
        try {
            // Given
            opChain = new OperationChain<>();

            // When
            final String overview = opChain.toOverviewString();

            // Then
            assertThat(overview).isEqualTo("OperationChain[]");
        } finally {
            CloseableUtil.close(opChain);
        }
    }
}
