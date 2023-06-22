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

package uk.gov.gchq.gaffer.gafferpop.service;

import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import uk.gov.gchq.gaffer.gafferpop.GafferPopElement;
import uk.gov.gchq.gaffer.gafferpop.GafferPopGraph;
import uk.gov.gchq.gaffer.gafferpop.generator.GafferPopElementGenerator;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;

import java.util.Arrays;
import java.util.Map;

public class GafferPopNamedOperationService<I, R> implements Service<I, R> {
    private final GafferPopGraph graph;

    public GafferPopNamedOperationService(final GafferPopGraph graph) {
        this.graph = graph;
    }

    @Override
    public Type getType() {
        return Type.Start;
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        if (params.containsKey("execute")) {
            final String name = (String) params.get("execute");
            Iterable<NamedOperationDetail> namedOps = graph.execute(new OperationChain.Builder().first(new GetAllNamedOperations()).build());
            for (final NamedOperationDetail namedOp : namedOps) {
                if (namedOp.getOperationName().equals(name)) {
                    if (namedOp.getOperationChainWithDefaultParams().getOutputClass().equals(Iterable.class)) {
                        return (CloseableIterator<R>) CloseableIterator.of(graph.execute(new OperationChain.Builder()
                                .first(new NamedOperation.Builder()
                                        .name(name)
                                        .build())
                                .then(new GenerateObjects.Builder<GafferPopElement>()
                                        .generator(new GafferPopElementGenerator(graph))
                                        .build())
                                .build()).iterator());
                    } else {
                        return (CloseableIterator<R>) CloseableIterator.of(Arrays.asList(graph.execute(new OperationChain.Builder()
                                .first(new NamedOperation.Builder()
                                        .name(name)
                                        .build())
                                .build())).iterator());
                    }
                }
            }
            throw new IllegalStateException("Named Operation not found");
        } else if (params.containsKey("add")) {
            final Map<String, Object> addParams = (Map) params.get("add");
            graph.execute(new OperationChain.Builder()
                    .first(new AddNamedOperation.Builder()
                            .name((String) addParams.get("name"))
                            .operationChain((String) addParams.get("opChain"))
                            .build())
                    .build());
            return CloseableIterator.empty();
        } else {
            throw new IllegalStateException("Missing parameter");
        }
    }

}
