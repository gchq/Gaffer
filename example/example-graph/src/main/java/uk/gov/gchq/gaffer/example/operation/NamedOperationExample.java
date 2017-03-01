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
package uk.gov.gchq.gaffer.example.operation;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import java.util.Collections;

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
                        .first(new GetAdjacentEntitySeeds.Builder()
                                .inOutType(GetOperation.IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .then(new GetAdjacentEntitySeeds.Builder()
                                .inOutType(GetOperation.IncludeIncomingOutgoingType.OUTGOING)
                                .deduplicate(true)
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

    public CloseableIterable<NamedOperation> getAllNamedOperations() {
        // ---------------------------------------------------------
        final GetAllNamedOperations operation = new GetAllNamedOperations();
        // ---------------------------------------------------------

        return runExample(operation);
    }

    public Iterable<EntitySeed> runNamedOperation() {
        // ---------------------------------------------------------
        final NamedOperation operation = new NamedOperation.Builder()
                .name("2-hop")
                .seeds(Collections.singletonList(new EntitySeed(2)))
                .build();
        // ---------------------------------------------------------

        return (Iterable) runExample(operation);
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
