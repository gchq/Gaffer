package uk.gov.gchq.gaffer.rest.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;

import java.util.LinkedList;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController("gremlin")
@RequestMapping(path = "/rest/gremlin")
public class GremlinController {

    private ConcurrentBindings bindings = new ConcurrentBindings();
    private GafferPopGraph gafferPopGraph;

    @Autowired
    public GremlinController(GraphTraversalSource g) {
        bindings.putIfAbsent("g", g);
        gafferPopGraph = (GafferPopGraph) g.getGraph();
    }

    @PostMapping(
        path = "/explain",
        consumes = TEXT_PLAIN_VALUE,
        produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(
        summary = "Explain a Gremlin Query",
        description = "Provides an explanation of what Gaffer operations are performed for given Gremlin query")
    public String explain(@RequestBody String gremlinQuery) {

        JSONObject response =  new JSONObject();
        try (GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(bindings).create()) {
            // Execute the query note this will actually run the query which we need
            // as Gremlin will skip steps if there is no input from the previous ones
            gremlinExecutor.eval(gremlinQuery).join();

            // Get the chain and reset the variables
            response = getGafferPopExplanation(gafferPopGraph);
            gafferPopGraph.setDefaultVariables((GafferPopGraphVariables) gafferPopGraph.variables());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new GafferRuntimeException("Failed to evaluate Gremlin query: " + e.getMessage(), e);
        }

        return response.toString();
    }

    /**
     * Gets an explanation of the last chain of operations ran on
     * a GafferPop graph. This essentially shows how a Gremlin
     * query mapped to a Gaffer operation chain
     *
     * @param graph The GafferPop graph
     * @return A JSON payload with an overview and full JSON representation of the
     *         chain in.
     */
    public static JSONObject getGafferPopExplanation(GafferPopGraph graph) {
        JSONObject result = new JSONObject();
        // Get the last operation chain rain
        LinkedList<Operation> operations = new LinkedList<>();
        ((GafferPopGraphVariables) graph.variables())
                .getLastOperationChain()
                .getOperations()
                .forEach(op -> {
                    if (op instanceof OperationChain) {
                        operations.addAll(((OperationChain) op).flatten());
                    } else {
                        operations.add(op);
                    }
                });
        OperationChain<?> flattenedChain = new OperationChain<>(operations);
        String overview = flattenedChain.toOverviewString();

        result.put("overview", overview);
        try {
            result.put("chain", new JSONObject(new String(JSONSerialiser.serialise(flattenedChain))));
        } catch (SerialisationException e) {
            result.put("chain", "FAILED TO SERIALISE OPERATION CHAIN");
        }

        return result;
    }

}
