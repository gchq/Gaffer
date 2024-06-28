package uk.gov.gchq.gaffer.rest.handler;

import java.util.AbstractMap.SimpleEntry;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import scala.Byte;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;

public class GremlinWebSocketHandler extends AbstractWebSocketHandler {

    private GraphTraversalSource g;

    private Map<String, MessageSerializer<?>> serialisers = Stream.of(
                new SimpleEntry<>("*/*", new GraphBinaryMessageSerializerV1()),
                new SimpleEntry<>("application/json", new GraphSONMessageSerializerV3()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));


    public GremlinWebSocketHandler(GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        MessageTextSerializer<?> serialiser = (MessageTextSerializer<?>) serialisers.get("*/*");
        RequestMessage request = serialiser.deserializeRequest(message.getPayload());
        final ResponseMessage response = handleGremlinRequest(request);
        session.sendMessage(new TextMessage(serialiser.serializeResponseAsString(response, null)));
	}

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
        MessageTextSerializer<?> serialiser = (MessageTextSerializer<?>) serialisers.get("application/json");

        ByteBuf byteBuf = convertToByteBuf(message);

        // Read the start bytes to find the type to correctly deserialise
        byte[] bytes = new byte[byteBuf.readByte()];
        byteBuf.readBytes(bytes);
        System.out.println("-----------------------------------------------------------");
        System.out.println(new String(bytes, StandardCharsets.UTF_8));

        // Deserialise the request ensuring to discard the already read bytes
        RequestMessage request = serialiser.deserializeRequest(byteBuf.discardReadBytes());

        final ResponseMessage response = handleGremlinRequest(request);
        session.sendMessage(new TextMessage(serialiser.serializeResponseAsString(response, ByteBufAllocator.DEFAULT)));
	}


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

    private ResponseMessage handleGremlinRequest(RequestMessage requestMessage) {
        final UUID requestId = requestMessage.getRequestId();
        ResponseMessage responseMessage = ResponseMessage.build(requestId).code(ResponseStatusCode.SERVER_ERROR).create();

        // Execute the query
        ConcurrentBindings bindings = new ConcurrentBindings();
        bindings.putIfAbsent("g", g);

        try (GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(bindings).create()) {
            Object result = gremlinExecutor.eval(
                    requestMessage.getArg(Tokens.ARGS_GREMLIN),
                    requestMessage.getArg(Tokens.ARGS_LANGUAGE),
                    requestMessage.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
                    requestMessage.getArgOrDefault(Tokens.ARGS_EVAL_TIMEOUT, null),
                    null)
                    .get();
            System.out.println("-----------------------------------------------------------");
            System.out.println(result);

            // Build the response
            responseMessage = ResponseMessage.build(requestId)
                    .code(ResponseStatusCode.SUCCESS)
                    .result(result).create();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new GafferRuntimeException("Failed to execute Gremlin query: " + e.getMessage(), e);
        }

        return responseMessage;
    }

}
