/*
 * Copyright 2024-2025 Crown Copyright
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3;
import org.json.JSONObject;
import org.opencypher.gremlin.server.jsr223.CypherPlugin;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.translator.Translator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import uk.gov.gchq.gaffer.commonutil.otel.OtelUtil;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.APPLICATION_NDJSON;
import static org.springframework.http.MediaType.APPLICATION_NDJSON_VALUE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@RestController
@Tag(name = "gremlin")
@RequestMapping("/rest/gremlin")
public class GremlinController {
    /**
     * The mapper for converting to GraphSONv3.
     */
    public static final GraphSONMapper GRAPHSON_V3_MAPPER = GraphSONMapper.build()
            .version(GraphSONVersion.V3_0)
            .addCustomModule(GraphSONXModuleV3.build()).create();

    /**
     * Writer for writing GraphSONv3 output to output streams.
     */
    public static final GraphSONWriter GRAPHSON_V3_WRITER = GraphSONWriter.build()
            .mapper(GRAPHSON_V3_MAPPER)
            .wrapAdjacencyList(true).create();

    // Keys for response explain JSON
    public static final String EXPLAIN_OVERVIEW_KEY = "overview";
    public static final String EXPLAIN_OP_CHAIN_KEY = "chain";
    public static final String EXPLAIN_GREMLIN_KEY = "gremlin";

    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinController.class);
    private static final String GENERAL_ERROR_MSG = "Gremlin query failed: ";
    // Shared thread pool for executing gremlin queries in
    private final ExecutorService executorService = Context.taskWrapping(Executors.newFixedThreadPool(8));
    private final AbstractUserFactory userFactory;
    private final Long requestTimeout;
    private final GafferPopGraph graph;
    private final Map<String, Map<String, Object>> plugins = new HashMap<>();

    @Autowired
    public GremlinController(final GafferPopGraph graph, final AbstractUserFactory userFactory, final Long requestTimeout) {
        this.graph = graph;
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
        return runGremlinQuery(gremlinQuery, httpHeaders).getRight().toString();
    }

    /**
     * Endpoint for running a gremlin groovy query, will respond with an output
     * stream of GraphSONv3 JSON.
     *
     * @param httpHeaders  The request headers.
     * @param gremlinQuery The gremlin groovy query.
     * @return A response output stream of GraphSONv3.
     *
     * @throws IOException If issue writing output.
     */
    @PostMapping(path = "/execute", consumes = TEXT_PLAIN_VALUE, produces = APPLICATION_NDJSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(
        summary = "Run a Gremlin Query",
        description = "Runs a Gremlin Groovy script and outputs the result as GraphSONv3 JSON")
    public ResponseEntity<StreamingResponseBody> execute(
            @RequestHeader final HttpHeaders httpHeaders,
            @RequestBody final String gremlinQuery) throws IOException {

        HttpStatus status = HttpStatus.OK;
        StreamingResponseBody responseBody;
        try {
            Object result = runGremlinQuery(gremlinQuery, httpHeaders).getLeft();
            // Write to output stream for response
            responseBody = os -> GRAPHSON_V3_WRITER.writeObject(os, result);
        } catch (final Exception e) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
            responseBody = os -> os.write(
                new JSONObject().put("simpleMessage", e.getMessage()).toString().getBytes(StandardCharsets.UTF_8));
        }

        return ResponseEntity.status(status)
                .contentType(APPLICATION_NDJSON)
                .body(responseBody);
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
        description = "Translates a Cypher query to Gremlin and outputs an explanation of what Gaffer operations " +
                      "were executed on the graph, note will always append a '.toList()' to the translation")
    public String cypherExplain(@RequestHeader final HttpHeaders httpHeaders, @RequestBody final String cypherQuery) {

        final CypherAst ast = CypherAst.parse(cypherQuery);
        // Translate the cypher to gremlin, always add a .toList() otherwise Gremlin wont execute it as its lazy
        final String translation = ast.buildTranslation(
            Translator.builder().gremlinGroovy().enableCypherExtensions().build()) + ".toList()";

        JSONObject response = runGremlinQuery(translation, httpHeaders).getRight();
        response.put(EXPLAIN_GREMLIN_KEY, translation);
        return response.toString();
    }

    /**
     * Endpoint for running a cypher query through gremlin, will respond with an
     * output stream of GraphSONv3 JSON.
     *
     * @param httpHeaders The request headers.
     * @param cypherQuery The cypher query.
     * @return The output stream of GraphSONv3.
     *
     * @throws IOException If issue writing output.
     */
    @PostMapping(path = "/cypher/execute", consumes = TEXT_PLAIN_VALUE, produces = APPLICATION_NDJSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(
        summary = "Run a Cypher Query",
        description = "Translates a Cypher query to Gremlin and executes it returning a GraphSONv3 JSON result. " +
                      "Note will always append a '.toList()' to the translation")
    public ResponseEntity<StreamingResponseBody> cypherExecute(
            @RequestHeader final HttpHeaders httpHeaders,
            @RequestBody final String cypherQuery) throws IOException {

        HttpStatus status = HttpStatus.OK;
        StreamingResponseBody responseBody;
        try {
            final CypherAst ast = CypherAst.parse(cypherQuery);
            // Translate the cypher to gremlin, always add a .toList() otherwise Gremlin
            // wont execute it as its lazy
            final String translation = ast.buildTranslation(
                Translator.builder().gremlinGroovy().enableCypherExtensions().build()) + ".toList()";
            // Run Query
            Object result = runGremlinQuery(translation, httpHeaders).getLeft();
            // Write to output stream for response
            responseBody = os -> GRAPHSON_V3_WRITER.writeObject(os, result);
        } catch (final Exception e) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
            responseBody = os -> os.write(
                new JSONObject().put("simpleMessage", e.getMessage()).toString().getBytes(StandardCharsets.UTF_8));
        }

        return ResponseEntity.status(status)
                .contentType(APPLICATION_NDJSON)
                .body(responseBody);
    }

    /**
     * Gets an explanation of the last chain of operations ran on a GafferPop graph.
     * This essentially shows how a Gremlin query mapped to a Gaffer operation
     * chain.
     * Note due to how Gaffer maps to Tinkerpop some filtering steps in the Gremlin
     * query may be absent from the operation chains in the explain as it may have
     * been done in the Tinkerpop framework instead.
     *
     * @param graphInstance The GafferPop graph instance.
     * @return A JSON payload with an overview and full JSON representation of the
     *         chain in.
     */
    public static JSONObject getGafferPopExplanation(final GafferPopGraph graphInstance) {
        JSONObject result = new JSONObject();
        // Get the last operation chain that ran
        List<Operation> operations = new ArrayList<>();
        ((GafferPopGraphVariables) graphInstance.variables())
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
     * @param graphInstance The graph instance to bind the traversal to.
     * @return The set-up {@link GremlinExecutor}.
     */
    private GremlinExecutor setUpExecutor(final GafferPopGraph graphInstance) {
        final ConcurrentBindings bindings = new ConcurrentBindings();

        // Set up the executor
        bindings.putIfAbsent("g", graphInstance.traversal());
        return GremlinExecutor.build()
                .addPlugins("gremlin-groovy", plugins)
                .evaluationTimeout(requestTimeout)
                .executorService(executorService)
                .globalBindings(bindings).create();
    }

    /**
     * Executes a given Gremlin query and returns the result along with an
     * explanation.
     *
     * @param gremlinQuery The Gremlin groovy query.
     * @param httpHeaders  The headers from the request for the
     *                     {@link AbstractUserFactory}.
     * @return A pair with result left and explain right.
     */
    private Pair<Object, JSONObject> runGremlinQuery(final String gremlinQuery, final HttpHeaders httpHeaders) {
        // We can't reuse the existing graph instance as we need to set variables
        // specific to this query only.
        final GafferPopGraph graphInstance = graph.newInstance();

        // Hooks for user auth
        userFactory.setHttpHeaders(httpHeaders);
        User user = userFactory.createUser();
        graphInstance.variables().set(GafferPopGraphVariables.USER, user);

        // OpenTelemetry hooks
        Span span = OtelUtil.startSpan(
            this.getClass().getName(), "Gremlin Request: " + UUID.nameUUIDFromBytes(gremlinQuery.getBytes(StandardCharsets.UTF_8)));
        span.setAttribute(OtelUtil.GREMLIN_QUERY_ATTRIBUTE, gremlinQuery);

        if (user != null) {
            span.setAttribute(OtelUtil.USER_ATTRIBUTE, user.getUserId());
        } else {
            LOGGER.warn("Could not find Gaffer user for OTEL. Using default.");
            span.setAttribute(OtelUtil.USER_ATTRIBUTE, "unknownGremlinUser");
        }

        // Pair to hold the result and explain
        MutablePair<Object, JSONObject> pair = new MutablePair<>(null, new JSONObject());

        try (Scope scope = span.makeCurrent();
                GremlinExecutor gremlinExecutor = setUpExecutor(graphInstance)) {
            // Execute the query
            Object result = gremlinExecutor.eval(gremlinQuery).get();

            // Store the result and explain for returning
            pair.setLeft(result);
            pair.setRight(getGafferPopExplanation(graphInstance));

            // Provide an debug explanation for the query that just ran
            span.addEvent("Request complete");
            LOGGER.debug("{}", pair.getRight());

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw new GafferRuntimeException(GENERAL_ERROR_MSG + e.getMessage(), e);
        } catch (final ExecutionException e) {
            span.setStatus(StatusCode.ERROR, e.getCause().getMessage());
            span.recordException(e.getCause());
            throw new GafferRuntimeException(GENERAL_ERROR_MSG + e.getCause().getMessage(), e);
        } catch (final Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw new GafferRuntimeException(GENERAL_ERROR_MSG + e.getMessage(), e);
        } finally {
            graphInstance.setDefaultVariables();
            span.end();
        }

        return pair;
    }
}
