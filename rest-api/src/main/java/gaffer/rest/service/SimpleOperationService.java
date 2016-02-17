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

package gaffer.rest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.graph.Graph;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetEdgesBySeed;
import gaffer.operation.impl.get.GetElementsSeed;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.operation.impl.get.GetRelatedEntities;
import gaffer.rest.GraphFactory;

/**
 * An implementation of {@link gaffer.rest.service.IOperationService}. By default it will use a singleton
 * {@link gaffer.graph.Graph} generated using the {@link gaffer.rest.GraphFactory}.
 * All operations are simple delegated to the graph.
 * Pre and post operation hooks are available by extending this class and implementing preOperationHook and/or
 * postOperationHook.
 */
public class SimpleOperationService implements IOperationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleOperationService.class);
    private final GraphFactory graphFactory;

    public SimpleOperationService() {
        this(new GraphFactory(true));
    }

    public SimpleOperationService(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Override
    public Object execute(final OperationChain opChain) {
        return execute(opChain, false);
    }

    @Override
    public Object generateObjects(final GenerateObjects operation) {
        return execute((GenerateObjects<?, ?>) operation);
    }

    @Override
    public Iterable<Element> generateElements(final GenerateElements operation) {
        return execute((GenerateElements<?>) operation);
    }

    @Override
    public void addElements(final AddElements operation) {
        execute(operation);
    }

    @Override
    public Iterable<Element> getElementsBySeed(final GetElementsSeed<ElementSeed, Element> operation, final Integer n) {
        return executeGet(operation, n);
    }

    @Override
    public Iterable<Element> getRelatedElements(final GetRelatedElements<ElementSeed, Element> operation, final Integer n) {
        return executeGet(operation, n);
    }

    @Override
    public Iterable<Entity> getEntitiesBySeed(final GetEntitiesBySeed operation, final Integer n) {
        return executeGet(operation, n);
    }

    @Override
    public Iterable<Entity> getRelatedEntities(final GetRelatedEntities operation, final Integer n) {
        return executeGet(operation, n);
    }

    @Override
    public Iterable<Edge> getEdgesBySeed(final GetEdgesBySeed operation, final Integer n) {
        return executeGet(operation, n);
    }

    @Override
    public Iterable<Edge> getRelatedEdges(final GetRelatedEdges operation, final Integer n) {
        return executeGet(operation, n);
    }

    @Override
    public Iterable<EntitySeed> getAdjacentEntitySeeds(final GetAdjacentEntitySeeds operation, final Integer n) {
        return executeGet(operation, n);
    }

    protected void preOperationHook(final OperationChain<?> opChain) {
        // no action by default
    }

    protected void postOperationHook(final OperationChain<?> opChain) {
        // no action by default
    }

    protected Graph getGraph() {
        return graphFactory.getGraph();
    }

    protected <OUTPUT> OUTPUT execute(final Operation<?, OUTPUT> operation) {
        return execute(new OperationChain<>(operation), false);
    }

    protected <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> opChain, final boolean async) {
        preOperationHook(opChain);

        if (async) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        graphFactory.getGraph().execute(opChain);
                    } catch (OperationException e) {
                        LOGGER.error("Error executing opChain", e);
                    } finally {
                        postOperationHook(opChain);
                    }
                }
            }).start();
            return null;
        } else {
            try {
                return graphFactory.getGraph().execute(opChain);
            } catch (OperationException e) {
                throw new RuntimeException("Error executing opChain", e);
            } finally {
                postOperationHook(opChain);
            }
        }
    }

    protected <OUTPUT> Iterable<OUTPUT> executeGet(final Operation<?, Iterable<OUTPUT>> operation, final Integer n) {
        return null != n ? Iterables.limit(execute(operation), n) : execute(operation);
    }

}
