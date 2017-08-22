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
package uk.gov.gchq.gaffer.doc.dev.walkthrough;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.doc.user.generator.RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.Map;

public class NamedOperations extends DevWalkthrough {
    public NamedOperations() {
        super("NamedOperations", "RoadAndRoadUseWithTimesAndCardinalities");
    }

    public CloseableIterable<? extends Element> run() throws OperationException, IOException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();
        // ---------------------------------------------------------

        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------

        // [add] Create a data generator and add the edges to the graph using an operation chain consisting of:
        // generateElements - generating edges from the data (note these are directed edges)
        // addElements - add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator())
                        .input(IOUtils.readLines(StreamUtil.openStream(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/data.txt")))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------

        // [add named operation] create an operation chain to be executed as a named operation
        // ---------------------------------------------------------
        final AddNamedOperation addOperation = new AddNamedOperation.Builder()
                .operationChain(new OperationChain.Builder()
                        .first(new GetElements.Builder()
                                .view(new View.Builder()
                                        .edge("RoadUse")
                                        .build())
                                .build())
                        .then(new Limit.Builder<>().resultLimit(10).build())
                        .build())
                .description("named operation limit query")
                .name("2-limit")
                .readAccessRoles("read-user")
                .writeAccessRoles("write-user")
                .overwrite()
                .build();

        graph.execute(addOperation, user);
        // ---------------------------------------------------------

        // [get all named operations] Get all named operations
        // ---------------------------------------------------------
        final CloseableIterable<NamedOperationDetail> details = graph.execute(new GetAllNamedOperations(), user);

        // ---------------------------------------------------------
        for (final NamedOperationDetail detail : details) {
            log("ALL_NAMED_OPERATIONS", detail.toString());
        }

        // [create named operation] create the named operation
        // ---------------------------------------------------------
        final NamedOperation<EntityId, CloseableIterable<? extends Element>> operation =
                new NamedOperation.Builder<EntityId, CloseableIterable<? extends Element>>()
                        .name("2-limit")
                        .input(new EntitySeed("10"))
                        .build();
        // ---------------------------------------------------------

        // [execute named operation] Get the results
        // ---------------------------------------------------------
        final CloseableIterable<? extends Element> results = graph.execute(operation, user);

        // ---------------------------------------------------------
        for (final Object result : results) {
            log("NAMED_OPERATION_RESULTS", result.toString());
        }

        // [add named operation with parameters] create an operation chain to be executed as a named operation
        // with parameters
        // ---------------------------------------------------------
        String opChainString = "{" +
                "  \"operations\" : [ {" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetElements\"," +
                "    \"view\" : {" +
                "      \"edges\" : {" +
                "        \"RoadUse\" : { }" +
                "      }," +
                "      \"entities\" : { }" +
                "    }" +
                "  }, {" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.Limit\"," +
                "    \"resultLimit\" : \"${limitParam}\"" +
                "  } ]" +
                "}";

        ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue(1L)
                .description("Limit param")
                .valueClass(Long.class)
                .build();
        Map<String, ParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("limitParam", param);

        final AddNamedOperation addOperationWithParams = new AddNamedOperation.Builder()
                .operationChain(opChainString)
                .description("named operation limit query")
                .name("custom-limit")
                .readAccessRoles("read-user")
                .writeAccessRoles("write-user")
                .parameters(paramDetailMap)
                .overwrite()
                .build();

        graph.execute(addOperationWithParams, user);
        // ---------------------------------------------------------

        // [create named operation with parameters] create the named operation with a parameter
        // ---------------------------------------------------------
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("limitParam", 3L);

        final NamedOperation<EntityId, CloseableIterable<? extends Element>> operationWithParams =
                new NamedOperation.Builder<EntityId, CloseableIterable<? extends Element>>()
                        .name("custom-limit")
                        .input(new EntitySeed("10"))
                        .parameters(paramMap)
                        .build();
        // ---------------------------------------------------------

        // [execute named operation with parameters] Get the results
        // ---------------------------------------------------------

        final CloseableIterable<? extends Element> namedOperationResults = graph.execute(operationWithParams, user);
        // ---------------------------------------------------------


        for (final Object result : namedOperationResults) {
            log("NAMED_OPERATION_WITH_PARAMETER_RESULTS", result.toString());
        }

        return namedOperationResults;
    }

    public static void main(final String[] args) throws OperationException, IOException {
        final NamedOperations walkthrough = new NamedOperations();
        walkthrough.run();
    }
}
