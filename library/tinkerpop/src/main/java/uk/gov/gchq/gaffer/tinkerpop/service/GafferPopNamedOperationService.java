/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.service;

import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopElement;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferPopElementGenerator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * Service for running Gaffer Named Operations
 *
 * <p>This service should be called at the start
 * of a traversal.</p>
 *
 * @param <I> Ignored
 * @param <R> Ignored
 *
 * @see <a href="https://tinkerpop.apache.org/docs/3.7.0/reference/#call-step">
 * Tinkerpop Call Step Documentation</a>
 */
public class GafferPopNamedOperationService<I, R> implements Service<I, R> {
    private final GafferPopGraph graph;

    public GafferPopNamedOperationService(final GafferPopGraph graph) {
        this.graph = graph;
    }

    @Override
    public Type getType() {
        return Type.Start;
    }

    /**
     * Executes the Service, either calling an existing Named Operation or
     * adding a new Named Operation.
     *
     * @param ctx Unused
     * @param params {@link Map} containing a key of either "execute" or
     * "add" and value with name to execute or operation chain to add
     * @return {@link CloseableIterator} with results or empty if adding
     * a Named Operation
     */
    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        if (params.containsKey("execute")) {
            return executeNamedOperation((String) params.get("execute"));
        } else if (params.containsKey("add")) {
            return addNamedOperation((Map<String, String>) params.get("add"));
        } else {
            throw new IllegalStateException("Missing parameter");
        }
    }

    protected CloseableIterator<R> executeNamedOperation(final String name) {
        // Fetch details for the requested Named Operation or throw an exception if it's not found
        Iterable<NamedOperationDetail> allNamedOps = graph.execute(new OperationChain.Builder().first(new GetAllNamedOperations()).build());
        NamedOperationDetail namedOperation = StreamSupport.stream(allNamedOps.spliterator(), false)
                .filter((NamedOperationDetail nd) -> nd.getOperationName().equals(name)).findFirst()
                .orElseThrow(() -> new IllegalStateException("Named Operation not found"));

        // If the Named Operation outputs an Iterable, then execute and convert Elements to GafferPopElement
        // Else execute and wrap the output as an iterator
        if (namedOperation.getOperationChainWithDefaultParams().getOutputClass().equals(Iterable.class)) {
            return (CloseableIterator<R>) CloseableIterator.of(graph.execute(new OperationChain.Builder()
                    .first(new NamedOperation.Builder()
                            .name(name)
                            .build())
                    .then(new GenerateObjects.Builder<GafferPopElement>()
                            .generator(new GafferPopElementGenerator(graph))
                            .build())
                    .build()).iterator());
        } else {
            return CloseableIterator.of(Arrays.asList(graph.execute(new OperationChain.Builder()
                    .first(new NamedOperation.Builder<Void, R>()
                            .name(name)
                            .build())
                    .build())).iterator());
        }
    }

    protected CloseableIterator<R> addNamedOperation(final Map<String, String> addParams) {
        graph.execute(new OperationChain.Builder()
                .first(new AddNamedOperation.Builder()
                        .name(addParams.get("name"))
                        .operationChain(addParams.get("opChain"))
                        .build())
                .build());
        return CloseableIterator.empty();
    }
}
