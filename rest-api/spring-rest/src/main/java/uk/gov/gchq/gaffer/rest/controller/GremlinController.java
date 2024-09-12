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

import io.opentelemetry.context.Context;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.json.JSONObject;
import org.opencypher.gremlin.server.jsr223.CypherPlugin;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController
@Tag(name = "gremlin")
@RequestMapping("/rest/gremlin")
public class GremlinController {


    // Keys for response explain JSON
    public static final String EXPLAIN_OVERVIEW_KEY = "overview";
    public static final String EXPLAIN_OP_CHAIN_KEY = "chain";
    public static final String EXPLAIN_GREMLIN_KEY = "gremlin";

    private static final String GENERAL_ERROR_MSG = "Failed to evaluate Gremlin query: ";


    private final ConcurrentBindings bindings = new ConcurrentBindings();
    private final ExecutorService executorService = Context.taskWrapping(Executors.newFixedThreadPool(4));
    private final Long requestTimeout;
    private final AbstractUserFactory userFactory;
    private final Graph graph;
    private final Map<String, Map<String, Object>> plugins = new HashMap<>();

    @Autowired
    public GremlinController(final GraphTraversalSource g, final AbstractUserFactory userFactory, final Long requestTimeout) {
        bindings.putIfAbsent("g", g);
        graph = g.getGraph();
        this.userFactory = userFactory;
        this.requestTimeout = requestTimeout;
        // Add cypher plugin so cypher functions can be used in queries
        plugins.put(CypherPlugin.class.getName(), new HashMap<>());
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
        preExecuteSetUp(httpHeaders);
        return runGremlinAndGetExplain(gremlinQuery).toString();
    }


    /**
     * Endpoint for running a gremlin groovy query.
     *
     * @param httpHeaders  The request headers.
     * @param gremlinQuery The gremlin groovy query.
     * @return Deserilised gremlin result
     * @throws SerializationException
     */
    @PostMapping(path = "/execute", consumes = TEXT_PLAIN_VALUE, produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(
        summary = "Run a Gremlin Query",
        description = "Runs a Gremlin query and outputs the result as a plain text result")
    public String execute(@RequestHeader final HttpHeaders httpHeaders, @RequestBody final String gremlinQuery) {
        preExecuteSetUp(httpHeaders);
        return runGremlinQuery(gremlinQuery).toString();
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

        JSONObject response = runGremlinAndGetExplain(translation);
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
     * Do some basic pre execute set up so the graph is ready for the gremlin
     * request to be executed.
     *
     * @param httpHeaders Headers for user auth
     */
    private void preExecuteSetUp(final HttpHeaders httpHeaders) {
        // Check we actually have a graph instance to use
        GafferPopGraph gafferPopGraph;
        if (graph instanceof EmptyGraph) {
            throw new GafferRuntimeException("There is no GafferPop Graph configured");
        } else {
            gafferPopGraph = (GafferPopGraph) graph;
        }
        gafferPopGraph.setDefaultVariables((GafferPopGraphVariables) gafferPopGraph.variables());
        // Hooks for user auth
        userFactory.setHttpHeaders(httpHeaders);
        graph.variables().set(GafferPopGraphVariables.USER, userFactory.createUser());
    }

    /**
     * Executes a given Gremlin query on the graph then formats a JSON response with
     * the executed Gaffer operations in.
     *
     * @param gremlinQuery The Gremlin groovy query.
     * @return JSON explanation.
     */
    private JSONObject runGremlinAndGetExplain(final String gremlinQuery) {
        GafferPopGraph gafferPopGraph = (GafferPopGraph) graph;
        JSONObject explain = new JSONObject();
        try (GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addPlugins("gremlin-groovy", plugins)
                .evaluationTimeout(requestTimeout)
                .executorService(executorService)
                .globalBindings(bindings).create()) {
            // Execute the query note this will actually run the query which we need
            // as Gremlin will skip steps if there is no input from the previous ones
            gremlinExecutor.eval(gremlinQuery).join();

            // Get the chain and reset the variables
            explain = getGafferPopExplanation(gafferPopGraph);
            gafferPopGraph.setDefaultVariables((GafferPopGraphVariables) gafferPopGraph.variables());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (final Exception e) {
            throw new GafferRuntimeException(GENERAL_ERROR_MSG + e.getMessage(), e);
        }

        return explain;
    }

    /**
     * Executes a given Gremlin query and returns the result.
     *
     * @param gremlinQuery The Gremlin groovy query.
     * @return The raw result.
     */
    private Object runGremlinQuery(final String gremlinQuery) {
        GafferPopGraph gafferPopGraph = (GafferPopGraph) graph;
        try (GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .addPlugins("gremlin-groovy", plugins)
                .evaluationTimeout(requestTimeout)
                .executorService(executorService)
                .globalBindings(bindings).create()) {

            // Execute the query
            Object result = gremlinExecutor.eval(gremlinQuery).join();
            gafferPopGraph.setDefaultVariables((GafferPopGraphVariables) gafferPopGraph.variables());
            return result;

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GafferRuntimeException(GENERAL_ERROR_MSG + e.getMessage(), e);
        } catch (final Exception e) {
            throw new GafferRuntimeException(GENERAL_ERROR_MSG + e.getMessage(), e);
        }
    }

}
