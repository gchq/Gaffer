package uk.gov.gchq.gaffer.rest.handler;

import java.util.AbstractMap.SimpleEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;


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
    private Map<String, MessageSerializer<?>> serialisers = Stream.of(
                new SimpleEntry<>(SerTokens.MIME_GRAPHBINARY_V1, new GraphBinaryMessageSerializerV1()),
                new SimpleEntry<>(SerTokens.MIME_GRAPHSON_V3, new GraphSONMessageSerializerV3()),
                new SimpleEntry<>(SerTokens.MIME_JSON, new GraphSONMessageSerializerV3()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private ConcurrentBindings bindings = new ConcurrentBindings();

    public GremlinWebSocketHandler(GraphTraversalSource g) {
        bindings.putIfAbsent("g", g);
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
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

        final ResponseMessage response = handleGremlinRequest(request);

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
    private ByteBuf convertToByteBuf(BinaryMessage message) {
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
     * Extracts the relevant information from a {@link RequestMessage} to execute a
     * Gremlin query on the currently bound traversal source for this class.
     * Formulates the result into a {@link ResponseMessage} to be sent back to the
     * client.
     * Will default to {@link ResponseStatusCode}.SERVER_ERROR if request fails.
     *
     * @param requestMessage The Gremlin request.
     * @return The response message containing the result.
     */
    private ResponseMessage handleGremlinRequest(RequestMessage requestMessage) {
        final UUID requestId = requestMessage.getRequestId();

        // Default to an error response
        ResponseMessage responseMessage = ResponseMessage.build(requestId).code(ResponseStatusCode.SERVER_ERROR).create();

        // Execute the query
        try (GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(bindings).create()) {
            Object result = gremlinExecutor.eval(
                    requestMessage.getArg(Tokens.ARGS_GREMLIN),
                    requestMessage.getArg(Tokens.ARGS_LANGUAGE),
                    requestMessage.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
                    requestMessage.getArgOrDefault(Tokens.ARGS_EVAL_TIMEOUT, null),
                    FunctionUtils.wrapFunction(output ->
                        // Need to replicate what TraversalOpProcessor does with the bytecode op. it converts
                        // results to Traverser so that GLVs can handle the results. Don't quite get the same
                        // benefit here because the bulk has to be 1 since we've already resolved the result
                        requestMessage.getOp().equals(Tokens.OPS_BYTECODE) ?
                                IteratorUtils.asList(output).stream().map(r -> new DefaultRemoteTraverser<Object>(r, 1)).collect(Collectors.toList()) :
                                IteratorUtils.asList(output)))
                    .get();

            // Build the response
            responseMessage = ResponseMessage.build(requestId)
                    .code(ResponseStatusCode.SUCCESS)
                    .result(result).create();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IllegalArgumentException e) {
            responseMessage = ResponseMessage.build(requestId)
                .code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST)
                .statusMessage(e.getMessage())
                .create();
        } catch (Exception e) {
            responseMessage = ResponseMessage.build(requestId)
                .code(ResponseStatusCode.SERVER_ERROR)
                .statusMessage(e.getMessage())
                .create();
        }

        return responseMessage;
    }



}
