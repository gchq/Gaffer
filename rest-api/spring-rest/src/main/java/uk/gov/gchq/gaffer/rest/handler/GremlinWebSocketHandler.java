/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.json.JSONObject;
import org.opencypher.gremlin.server.jsr223.CypherPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import uk.gov.gchq.gaffer.commonutil.otel.OtelUtil;
import uk.gov.gchq.gaffer.rest.controller.GremlinController;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Websocket handler for accepting and responding to Gremlin queries.
 * This enables an endpoint that acts like a Gremlin server which will
 * run requests on the current Gaffer graph via the GafferPop library.
 */
public class GremlinWebSocketHandler extends BinaryWebSocketHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinWebSocketHandler.class);

    /**
     * The default serialiser used for Gremlin queries if the serialiser for mime type is not found.
     */
    private static final MessageSerializer<?> DEFAULT_SERIALISER = new GraphBinaryMessageSerializerV1();

    // Mappings of mime types and serialisers
    private final Map<String, MessageSerializer<?>> serialisers = Stream.of(
            new SimpleEntry<>(SerTokens.MIME_GRAPHBINARY_V1, new GraphBinaryMessageSerializerV1()),
            new SimpleEntry<>(SerTokens.MIME_GRAPHSON_V3, new GraphSONMessageSerializerV3()),
            new SimpleEntry<>(SerTokens.MIME_JSON, new GraphSONMessageSerializerV3()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private final ExecutorService executorService = Context.taskWrapping(Executors.newFixedThreadPool(4));
    private final ConcurrentBindings bindings = new ConcurrentBindings();
    private final Long requestTimeout;
    private final AbstractUserFactory userFactory;
    private final Graph graph;
    private final Map<String, Map<String, Object>> plugins = new HashMap<>();

    /**
     * Constructor
     *
     * @param g The graph traversal source
     * @param userFactory The user factory
     * @param requestTimeout The timeout for gremlin requests
     */
    public GremlinWebSocketHandler(final GraphTraversalSource g, final AbstractUserFactory userFactory, final Long requestTimeout) {
        bindings.putIfAbsent("g", g);
        graph = g.getGraph();
        this.userFactory = userFactory;
        this.requestTimeout = requestTimeout;
        // Add cypher plugin so cypher functions can be used in queries
        plugins.put(CypherPlugin.class.getName(), new HashMap<>());
    }

    @Override
    protected void handleBinaryMessage(final WebSocketSession session, final BinaryMessage message) throws Exception {
        ByteBuf byteBuf = convertToByteBuf(message);

        // Read the start bytes to find the type to correctly deserialise
        byte[] bytes = new byte[byteBuf.readByte()];
        byteBuf.readBytes(bytes);
        String mimeType = new String(bytes, StandardCharsets.UTF_8);

        // Use the relevant serialiser for the mime type (cast to text is required to send response as String)
        MessageTextSerializer<?> serialiser = (MessageTextSerializer<?>) serialisers.getOrDefault(mimeType, DEFAULT_SERIALISER);
        LOGGER.debug("Using Tinkerpop serialiser: {}", serialiser.getClass().getSimpleName());

        // Deserialise the request ensuring to discard the already read bytes
        RequestMessage request = serialiser.deserializeRequest(byteBuf.discardReadBytes());

        // Handle and respond
        sendBinaryResponse(session, serialiser, handleGremlinRequest(session, request));
    }

    /**
     * Extracts the relevant information from a {@link RequestMessage} and validates
     * the Gremlin query requested before executing on the current graph. Formulates
     * the result into a {@link ResponseMessage} to be sent back to the client.
     *
     * @param session The current websocket session.
     * @param request The Gremlin request.
     * @return The response message containing the result.
     */
    private ResponseMessage handleGremlinRequest(final WebSocketSession session, final RequestMessage request) {
        final UUID requestId = request.getRequestId();
        ResponseMessage responseMessage;
        LOGGER.info("QUERY IS: {} ", request.getArgs().get(Tokens.ARGS_GREMLIN));

        // OpenTelemetry hooks
        Span span = OtelUtil.startSpan(this.getClass().getName(), "Gremlin Request: " + requestId.toString());
        span.setAttribute(OtelUtil.GREMLIN_QUERY_ATTRIBUTE, request.getArgs().get(Tokens.ARGS_GREMLIN).toString());

        // Execute the query
        try (Scope scope = span.makeCurrent();
                GremlinExecutor gremlinExecutor = getGremlinExecutor()) {
            // Set current headers for potential authorisation then set the user
            userFactory.setHttpHeaders(session.getHandshakeHeaders());
            User user = userFactory.createUser();
            graph.variables().set(GafferPopGraphVariables.USER, user);
            span.setAttribute(OtelUtil.USER_ATTRIBUTE, user.getUserId());

            // Run the query using the gremlin executor service
            Object result = gremlinExecutor.eval(
                    request.getArgs().get(Tokens.ARGS_GREMLIN),
                    request.getArg(Tokens.ARGS_LANGUAGE),
                    request.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
                    request.getArgOrDefault(Tokens.ARGS_EVAL_TIMEOUT, null),
                    FunctionUtils.wrapFunction(output ->
                    // Need to replicate what TraversalOpProcessor does with a bytecode op, it converts
                    // results to Traverser so that GLVs can handle the results. Don't quite get the same
                    // benefit here as the bulk has to be 1 since we've already resolved the result
                    request.getOp().equals(Tokens.OPS_BYTECODE)
                            ? IteratorUtils.asList(output).stream().map(r -> new DefaultRemoteTraverser<Object>(r, 1))
                                    .collect(Collectors.toList())
                            : IteratorUtils.asList(output)))
                .get();

            // Provide an debug explanation for the query that just ran
            span.addEvent("Request complete");
            if (graph instanceof GafferPopGraph) {
                JSONObject gafferOperationChain = GremlinController.getGafferPopExplanation((GafferPopGraph) graph);
                LOGGER.debug("{}", gafferOperationChain);
            }

            // Build the response
            responseMessage = ResponseMessage.build(requestId)
                    .code(ResponseStatusCode.SUCCESS)
                    .result(result).create();

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            responseMessage = ResponseMessage.build(requestId)
                    .code(ResponseStatusCode.SERVER_ERROR)
                    .statusMessage(e.getMessage()).create();
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
        } catch (final Exception e) {
            responseMessage = ResponseMessage.build(requestId)
                    .code(ResponseStatusCode.SERVER_ERROR)
                    .statusMessage(e.getMessage()).create();
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
        } finally {
            span.end();
        }

        return responseMessage;

    }

    /**
     * Serialises and sends a response using the current session.
     *
     * @param session Current websocket session
     * @param serialiser The serialiser to use for the message
     * @param response The response message
     * @throws IOException If fail to serialise or send
     */
    private void sendBinaryResponse(final WebSocketSession session, final MessageSerializer<?> serialiser, final ResponseMessage response) throws IOException {
        // Serialise response and read the bytes into a byte array
        ByteBuf responseByteBuf = serialiser.serializeResponseAsBinary(response, PooledByteBufAllocator.DEFAULT);
        byte[] responseBytes = new byte[responseByteBuf.readableBytes()];
        responseByteBuf.readBytes(responseBytes);
        // Send response
        session.sendMessage(new BinaryMessage(responseBytes));
    }

    /**
     * Simple method to help the conversion between {@link BinaryMessage}
     * and {@link ByteBuf} type.
     *
     * @param message the binary web socket message.
     * @return A netty byte buffer for the message.
     */
    private ByteBuf convertToByteBuf(final BinaryMessage message) {
        ByteBuffer byteBuffer = message.getPayload();

        if (byteBuffer.hasArray()) {
            return Unpooled.wrappedBuffer(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining());
        } else {
            byte[] byteArray = new byte[byteBuffer.remaining()];
            byteBuffer.get(byteArray);
            return Unpooled.wrappedBuffer(byteArray);
        }
    }

    /**
     * Returns a new gremlin executor. It's the responsibility of the caller to
     * ensure it is closed.
     *
     * @return Gremlin executor.
     */
    private GremlinExecutor getGremlinExecutor() {
        return GremlinExecutor.build()
                .globalBindings(bindings)
                .addPlugins("gremlin-groovy", plugins)
                .evaluationTimeout(requestTimeout)
                .executorService(executorService)
                .create();
    }

}
