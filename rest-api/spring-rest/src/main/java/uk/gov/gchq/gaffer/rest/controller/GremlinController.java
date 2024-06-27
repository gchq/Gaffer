package uk.gov.gchq.gaffer.rest.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.Collections;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseResult;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.springframework.beans.factory.annotation.Autowired;

@RestController
@RequestMapping("/gremlin")
public class GremlinController {

    private GraphTraversalSource g;

    @Autowired
    public GremlinController(GraphTraversalSource g) {
        this.g = g;
    }


    @PostMapping(path = "/execute")
    @Operation(summary = "Gremlin end point for Tinkerpop graph traversals")
    @RequestMapping(produces = APPLICATION_JSON_VALUE)
    public Object execute(@RequestBody String gremlinQuery) {
        System.out.println("-----------------------------------------------------------");
        System.out.println(g.V().toList().size());

        ConcurrentBindings bindings = new ConcurrentBindings();
        bindings.putIfAbsent("g", g);

        Object result = Collections.emptyIterator();

        try (GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(bindings).create()) {

            result = gremlinExecutor.eval(gremlinQuery).get();
            System.out.println("-----------------------------------------------------------");
            System.out.println(result);

            // Serialise the result before returning
            GraphSONMessageSerializerV3 serializer = new GraphSONMessageSerializerV3();
            ResponseResult responseResult = new ResponseResult(result, null);
            ResponseMessage message = ResponseMessage.build(UUID.randomUUID()).result(responseResult).create();
            return serializer.serializeResponseAsString(message, null);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new GafferRuntimeException("Failed to execute Gremlin query: " + e.getMessage(), e);
        }

        return result;
    }

}
