package uk.gov.gchq.gaffer.rest.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.ReferenceCountUtil;
import io.swagger.v3.oas.annotations.Operation;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseResult;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;

@RestController
public class GremlinController {

    private Map<String, MessageSerializer<?>> serialisers = Stream.of(
                new SimpleEntry<>("*/*", new GraphBinaryMessageSerializerV1()),
                new SimpleEntry<>("application/json", new GraphSONMessageSerializerV3()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private GraphTraversalSource g;

    @Autowired
    public GremlinController(GraphTraversalSource g) {
        this.g = g;
    }


    @PostMapping("/gremlin/explain")
    @Operation(summary = "Gremlin end point for Tinkerpop graph traversals")
    // @RequestMapping(produces = APPLICATION_JSON_VALUE)
    public Object execute(Object request) {

        Object result = Collections.emptyIterator();

        if (request instanceof FullHttpRequest) {
            final FullHttpRequest httpReq = (FullHttpRequest) request;

            final RequestMessage requestMessage;
            try {
                requestMessage = HttpHandlerUtil.getRequestMessageFromHttpRequest(httpReq, serialisers);
            } catch (SerializationException e) {
                ReferenceCountUtil.release(request);
                throw new GafferRuntimeException("Failed to serialise Gremlin query: " + e.getMessage(), e);
            }

            final UUID requestId = requestMessage.getRequestId();
            final String acceptMime = Optional.ofNullable(httpReq.headers().get(HttpHeaderNames.ACCEPT)).orElse("application/json");
            if (!serialisers.containsKey(acceptMime)) {
                ReferenceCountUtil.release(request);
                throw new GafferRuntimeException("No available Gremlin serialiser for http request type: " + acceptMime);
            }

            MessageSerializer<?> serialser = serialisers.get(acceptMime);

            // Release the original message as we will use the deserialised request from now on
            ReferenceCountUtil.release(request);

            System.out.println("-----------------------------------------------------------");
            String gremlinQuery = requestMessage.getArg(Tokens.ARGS_GREMLIN);
            System.out.println(gremlinQuery);

            ConcurrentBindings bindings = new ConcurrentBindings();
            bindings.putIfAbsent("g", g);


            try (GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(bindings).create()) {
                result = gremlinExecutor.eval(
                        requestMessage.getArg(Tokens.ARGS_GREMLIN),
                        requestMessage.getArg(Tokens.ARGS_LANGUAGE),
                        requestMessage.getArgOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
                        requestMessage.getArgOrDefault(Tokens.ARGS_EVAL_TIMEOUT,null),
                        null)
                    .get();
                System.out.println("-----------------------------------------------------------");
                System.out.println(result);

                // Serialise the result before returning
                final ResponseMessage responseMessage = ResponseMessage.build(requestId)
                    .code(ResponseStatusCode.SUCCESS)
                    .result(result).create();
                return serialser.serializeResponseAsBinary(responseMessage, null);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                throw new GafferRuntimeException("Failed to execute Gremlin query: " + e.getMessage(), e);
            }


        }

        return result;
    }

}
