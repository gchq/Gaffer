/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdgesBySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElementsBySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntitiesBySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedEntities;
import uk.gov.gchq.gaffer.rest.GraphFactory;
import uk.gov.gchq.gaffer.user.User;
import java.io.Closeable;
import java.io.IOException;

import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;

/**
 * An implementation of {@link uk.gov.gchq.gaffer.rest.service.IOperationService}. By default it will use a singleton
 * {@link uk.gov.gchq.gaffer.graph.Graph} generated using the {@link uk.gov.gchq.gaffer.rest.GraphFactory}.
 * All operations are simple delegated to the graph.
 * Pre and post operation hooks are available by extending this class and implementing preOperationHook and/or
 * postOperationHook.
 * <p>
 * By default queries will be executed with an UNKNOWN user containing no auths.
 * The createUser() method should be overridden and a {@link User} object should
 * be created from the http request.
 * </p>
 */
public class SimpleOperationService implements IOperationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleOperationService.class);
    private final GraphFactory graphFactory;
    public final ObjectMapper mapper = createDefaultMapper();

    public SimpleOperationService() {
        this(GraphFactory.createGraphFactory());
    }

    public SimpleOperationService(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Override
    public Object execute(final OperationChain opChain) {
        return execute(opChain, false);
    }

    @SuppressFBWarnings
    @Override
    public ChunkedOutput<String> executeChunked(final OperationChain opChain) {
        // Create chunked output instance
        final ChunkedOutput<String> output = new ChunkedOutput<>(String.class, "\r\n");

        // write chunks to the chunked output object
        new Thread() {
            public void run() {
                try {
                    final Object result = execute(opChain);
                    chunkResult(result, output);
                } finally {
                    IOUtils.closeQuietly(output);
                }
            }
        }.start();

        return output;
    }

    @Override
    public CloseableIterable<Object> generateObjects(final GenerateObjects<Element, Object> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Element> generateElements(final GenerateElements<ElementSeed> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Element> getElementsBySeed(final GetElementsBySeed<ElementSeed, Element> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Element> getRelatedElements(final GetRelatedElements<ElementSeed, Element> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Entity> getEntitiesBySeed(final GetEntitiesBySeed operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Entity> getRelatedEntities(final GetRelatedEntities<ElementSeed> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Edge> getEdgesBySeed(final GetEdgesBySeed operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Edge> getRelatedEdges(final GetRelatedEdges<ElementSeed> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<EntitySeed> getAdjacentEntitySeeds(final GetAdjacentEntitySeeds operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Element> getAllElements(final GetAllElements<Element> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Entity> getAllEntities(final GetAllEntities operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Edge> getAllEdges(final GetAllEdges operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Element> getElements(final GetElements<ElementSeed, Element> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Entity> getEntities(final GetEntities<ElementSeed> operation) {
        return execute(operation);
    }

    @Override
    public CloseableIterable<Edge> getEdges(final GetEdges<ElementSeed> operation) {
        return execute(operation);
    }

    @Override
    public void addElements(final AddElements operation) {
        execute(operation);
    }

    /**
     * Creates a {@link User} object containing information about the user
     * querying Gaffer.
     * By default this will return a user with id: UNKNOWN.
     * <p>
     * This method should be overridden for implementations of this API. The
     * user information should be fetched from the request.
     *
     * @return the user querying Gaffer.
     */

    protected User createUser() {
        return new User();
    }

    protected void preOperationHook(final OperationChain<?> opChain, final User user) {
        // no action by default
    }

    protected void postOperationHook(final OperationChain<?> opChain, final User user) {
        // no action by default
    }

    protected Graph getGraph() {
        return graphFactory.getGraph();
    }

    protected <OUTPUT> OUTPUT execute(final Operation<?, OUTPUT> operation) {
        return execute(new OperationChain<>(operation), false);
    }

    protected <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> opChain, final boolean async) {
        final User user = createUser();
        preOperationHook(opChain, user);

        if (async) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        graphFactory.getGraph().execute(opChain, user);
                    } catch (OperationException e) {
                        LOGGER.error("Error executing opChain", e);
                    } finally {
                        postOperationHook(opChain, user);
                    }
                }
            }).start();
            return null;
        } else {
            try {
                return graphFactory.getGraph().execute(opChain, user);
            } catch (OperationException e) {
                throw new RuntimeException("Error executing opChain", e);
            } finally {
                postOperationHook(opChain, user);
            }
        }
    }

    protected void chunkResult(final Object result, final ChunkedOutput<String> output) {
        if (result instanceof Iterable) {
            final Iterable itr = (Iterable) result;
            try {
                for (final Object item : itr) {
                    output.write(mapper.writeValueAsString(item));
                }
            } catch (final IOException ioe) {
                LOGGER.warn("IOException (chunks)", ioe);
            } finally {
                if (itr instanceof Closeable) {
                    IOUtils.closeQuietly(((Closeable) itr));
                }
            }
        } else {
            try {
                output.write(mapper.writeValueAsString(result));
            } catch (IOException ioe) {
                LOGGER.warn("IOException (chunks)", ioe);
            }
        }
    }
}
