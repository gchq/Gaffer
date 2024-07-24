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

package uk.gov.gchq.gaffer.rest.controller;

import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.json.JSONObject;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.translator.Translator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController
@Tag(name = "gremlin")
@RequestMapping("/rest/gremlin")
public class GremlinController {

    // Keys for response JSON
    public static final String EXPLAIN_OVERVIEW_KEY = "overview";
    public static final String EXPLAIN_OP_CHAIN_KEY = "chain";
    public static final String EXPLAIN_GREMLIN_KEY = "gremlin";

    private final ConcurrentBindings bindings = new ConcurrentBindings();
    private final AbstractUserFactory userFactory;
    private final Graph graph;

    @Autowired
    public GremlinController(final GraphTraversalSource g, final AbstractUserFactory userFactory) {
        bindings.putIfAbsent("g", g);
        graph = g.getGraph();
        this.userFactory = userFactory;
    }

    /**
     * Explains what Gaffer operations are run for a given gremlin query.
     *
     * @param httpHeaders The request headers.
     * @param gremlinQuery The gremlin groovy query.
     * @return JSON response with explanation in.
     */
    @PostMapping(path = "/explain", consumes = TEXT_PLAIN_VALUE, produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(
        summary = "Explain a Gremlin Query",
        description = "Runs a Gremlin query and outputs an explanation of what Gaffer operations were executed on the graph")
    public String explain(@RequestHeader final HttpHeaders httpHeaders, @RequestBody final String gremlinQuery) {
        return runGremlinAndGetExplain(gremlinQuery, httpHeaders).toString();
    }

    /**
     * Explains what Gaffer operations are ran for a given cypher query,
     * will translate to Gremlin using {@link CypherAst} before executing.
     *
     * @param httpHeaders The request headers.
     * @param cypherQuery Opencypher query.
     * @return JSON response with explanation in.
     */
    @PostMapping(path = "/cypher/explain", consumes = TEXT_PLAIN_VALUE, produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(
        summary = "Explain a Cypher Query Executed via Gremlin",
        description = "Translates a Cypher query to Gremlin and outputs an explanation of what Gaffer operations" +
                      "were executed on the graph, note will always append a '.toList()' to the translation")
    public String cypherExplain(@RequestHeader final HttpHeaders httpHeaders, @RequestBody final String cypherQuery) {

        final CypherAst ast = CypherAst.parse(cypherQuery);
        // Translate the cypher to gremlin, always add a .toList() otherwise Gremlin wont execute it as its lazy
        final String translation = ast.buildTranslation(Translator.builder().gremlinGroovy().enableCypherExtensions().build()) + ".toList()";

        JSONObject response = runGremlinAndGetExplain(translation, httpHeaders);
        response.put(EXPLAIN_GREMLIN_KEY, translation);
        return response.toString();
    }

    /**
     * Gets an explanation of the last chain of operations ran on a GafferPop graph.
     * This essentially shows how a Gremlin query mapped to a Gaffer operation
     * chain.
     * Note due to how Gaffer maps to Tinkerpop some filtering steps in the Gremlin
     * query may be absent from the operation chains in the explain as it may have
     * been done in the Tinkerpop framework instead.
     *
     * @param graph The GafferPop graph
     * @return A JSON payload with an overview and full JSON representation of the
     *         chain in.
     */
    public static JSONObject getGafferPopExplanation(final GafferPopGraph graph) {
        JSONObject result = new JSONObject();
        // Get the last operation chain that ran
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

        result.put(EXPLAIN_OVERVIEW_KEY, overview);
        try {
            result.put(EXPLAIN_OP_CHAIN_KEY, new JSONObject(new String(JSONSerialiser.serialise(flattenedChain), StandardCharsets.UTF_8)));
        } catch (final SerialisationException e) {
            result.put(EXPLAIN_OP_CHAIN_KEY, "FAILED TO SERIALISE OPERATION CHAIN");
        }

        return result;
    }

    /**
     * Executes a given Gremlin query on the graph then formats a JSON response with
     * the executed Gaffer operations in.
     *
     * @param gremlinQuery The Gremlin groovy query.
     * @param httpHeaders The headers for the request.
     * @return JSON explanation.
     */
    private JSONObject runGremlinAndGetExplain(final String gremlinQuery, final HttpHeaders httpHeaders) {
        // Check we actually have a graph instance to use
        GafferPopGraph gafferPopGraph;
        if (graph instanceof EmptyGraph) {
            throw new GafferRuntimeException("There is no GafferPop Graph configured");
        } else {
            gafferPopGraph = (GafferPopGraph) graph;
        }
        // Hooks for user auth
        userFactory.setHttpHeaders(httpHeaders);
        graph.variables().set(GafferPopGraphVariables.USER, userFactory.createUser());

        JSONObject explain = new JSONObject();
        try (GremlinExecutor gremlinExecutor = GremlinExecutor.build().globalBindings(bindings).create()) {
            gafferPopGraph.setDefaultVariables((GafferPopGraphVariables) gafferPopGraph.variables());
            // Execute the query note this will actually run the query which we need
            // as Gremlin will skip steps if there is no input from the previous ones
            gremlinExecutor.eval(gremlinQuery).join();

            // Get the chain and reset the variables
            explain = getGafferPopExplanation(gafferPopGraph);
            gafferPopGraph.setDefaultVariables((GafferPopGraphVariables) gafferPopGraph.variables());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final Exception e) {
            throw new GafferRuntimeException("Failed to evaluate Gremlin query: " + e.getMessage(), e);
        }

        return explain;
    }

}
