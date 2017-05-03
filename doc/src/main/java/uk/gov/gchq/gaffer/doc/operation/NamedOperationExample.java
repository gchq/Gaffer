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
package uk.gov.gchq.gaffer.doc.operation;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;

public class NamedOperationExample extends OperationExample {
    public static void main(final String[] args) {
        new NamedOperationExample().run();
    }

    public NamedOperationExample() {
        super(NamedOperation.class);
    }

    @Override
    public void runExamples() {
        addNamedOperation();
        getAllNamedOperations();
        runNamedOperation();
        deleteNamedOperation();
    }

    public void addNamedOperation() {
        // ---------------------------------------------------------
        final AddNamedOperation operation = new AddNamedOperation.Builder()
                .operationChain(new OperationChain.Builder()
                        .first(new GetAdjacentIds.Builder()
                                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .then(new GetAdjacentIds.Builder()
                                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .build())
                .description("2 hop query")
                .name("2-hop")
                .readAccessRoles("read-user")
                .writeAccessRoles("write-user")
                .overwrite()
                .build();
        // ---------------------------------------------------------

        runExampleNoResult(operation);
    }

    public CloseableIterable<NamedOperationDetail> getAllNamedOperations() {
        // ---------------------------------------------------------
        final GetAllNamedOperations operation = new GetAllNamedOperations();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public CloseableIterable<EntityId> runNamedOperation() {
        // ---------------------------------------------------------
        final NamedOperation<EntityId, CloseableIterable<EntityId>> operation =
                new NamedOperation.Builder<EntityId, CloseableIterable<EntityId>>()
                        .name("2-hop")
                        .input(new EntitySeed(2))
                        .build();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public void deleteNamedOperation() {
        // ---------------------------------------------------------
        final DeleteNamedOperation operation = new DeleteNamedOperation.Builder()
                .name("2-hop")
                .build();
        // ---------------------------------------------------------

        runExampleNoResult(operation);
    }
}
