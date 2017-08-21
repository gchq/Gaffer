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

import com.google.common.collect.Maps;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import java.util.Map;

public class NamedOperationExample extends OperationExample {
    public static void main(final String[] args) {
        new NamedOperationExample().run();
    }

    public NamedOperationExample() {
        super(NamedOperation.class, "See [Named Operations](https://github.com/gchq/Gaffer/wiki/Dev-Guide#namedoperations) for information on configuring named operations for your Gaffer graph.");
    }

    @Override
    public void runExamples() {
        // Clear the cache
        try {
            CacheServiceLoader.getService().clearCache("NamedOperation");
        } catch (CacheOperationException e) {
            throw new RuntimeException(e);
        }

        addNamedOperation();
        addNamedOperationWithParameter();
        getAllNamedOperations();
        runNamedOperation();
        runNamedOperationWithParameter();
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

        runExampleNoResult(operation, null);
    }

    public void addNamedOperationWithParameter() {
        // ---------------------------------------------------------
        final String opChainString = "{" +
                "    \"operations\" : [ {" +
                "      \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\"," +
                "      \"includeIncomingOutGoing\" : \"OUTGOING\"" +
                "    }, {" +
                "      \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds\"," +
                "      \"includeIncomingOutGoing\" : \"OUTGOING\"" +
                "    }, {" +
                "      \"class\" : \"uk.gov.gchq.gaffer.operation.impl.Limit\"," +
                "      \"resultLimit\" : \"${param1}\"" +
                "    }" +
                " ]" +
                "}";

        ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        Map<String, ParameterDetail> paramMap = Maps.newHashMap();
        paramMap.put("param1", param);

        final AddNamedOperation operation = new AddNamedOperation.Builder()
                .operationChain(opChainString)
                .description("2 hop query with settable limit")
                .name("2-hop-with-limit")
                .readAccessRoles("read-user")
                .writeAccessRoles("write-user")
                .parameters(paramMap)
                .overwrite()
                .build();
        // ---------------------------------------------------------

        runExampleNoResult(operation, null);
    }

    public CloseableIterable<NamedOperationDetail> getAllNamedOperations() {
        // ---------------------------------------------------------
        final GetAllNamedOperations operation = new GetAllNamedOperations();
        // ---------------------------------------------------------

        return runExample(operation, null);
    }

    public CloseableIterable<EntityId> runNamedOperation() {
        // ---------------------------------------------------------
        final NamedOperation<EntityId, CloseableIterable<EntityId>> operation =
                new NamedOperation.Builder<EntityId, CloseableIterable<EntityId>>()
                        .name("2-hop")
                        .input(new EntitySeed(1))
                        .build();
        // ---------------------------------------------------------

        return runExample(operation, null);
    }

    public CloseableIterable<EntityId> runNamedOperationWithParameter() {
        // ---------------------------------------------------------
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("param1", 2L);

        final NamedOperation<EntityId, CloseableIterable<EntityId>> operation =
                new NamedOperation.Builder<EntityId, CloseableIterable<EntityId>>()
                        .name("2-hop-with-limit")
                        .input(new EntitySeed(1))
                        .parameters(paramMap)
                        .build();
        // ---------------------------------------------------------

        return runExample(operation, null);
    }

    public void deleteNamedOperation() {
        // ---------------------------------------------------------
        final DeleteNamedOperation operation = new DeleteNamedOperation.Builder()
                .name("2-hop")
                .build();
        // ---------------------------------------------------------

        runExampleNoResult(operation, null);
    }
}
