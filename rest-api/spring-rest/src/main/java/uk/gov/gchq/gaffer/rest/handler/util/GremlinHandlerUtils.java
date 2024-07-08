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

package uk.gov.gchq.gaffer.rest.handler.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode.Instruction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.translator.Translator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.BinaryMessage;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class containing methods useful for processing Gremlin requests.
 */
public final class GremlinHandlerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinHandlerUtils.class);
    private static final GraphTraversalSource EMPTY_G = EmptyGraph.instance().traversal();
    private static final GremlinGroovyScriptEngine SCRIPT_ENGINE = new GremlinGroovyScriptEngine();

    // Rejection messages
    public static final String REJECT_BYTECODE = "User not authorized for bytecode requests on %s";
    public static final String REJECT_LAMBDA = "lambdas";
    public static final String REJECT_MUTATE = "the ReadOnlyStrategy";
    public static final String REJECT_OLAP = "the VertexProgramRestrictionStrategy";
    public static final String REJECT_STRING = "User not authorized for string-based requests.";

    private GremlinHandlerUtils() {
        // Utility class
    }

    /**
     * Checks the byte code for a cypher traversal request if found it
     * will return the translated bytecode otherwise it will simply return
     * the original bytecode.
     *
     * @param bytecode The bytecode to check.
     * @return The translated bytecode if cypher is present.
     */
    public static Bytecode translateCypherIfPresent(final Bytecode bytecode) {
        // Loop over the instructions in the query to see if any options have been passed
        for (final Instruction i : bytecode.getInstructions()) {
            if (!i.getOperator().equals("withStrategies")) {
                continue;
            }
            // The bytecode will have a proxy traversal with all the options in so extract
            if (i.getArguments()[0] instanceof TraversalStrategyProxy) {
                Map<String, Object> options = ((MapConfiguration) ((TraversalStrategyProxy<?>) i.getArguments()[0]).getConfiguration()).getMap();
                LOGGER.debug("Found options from traversal: {}", options);
                // Found cypher
                if (options.containsKey(GafferPopGraphVariables.CYPHER_KEY)) {
                    String cypherString = (String) options.get(GafferPopGraphVariables.CYPHER_KEY);
                    LOGGER.info("Translating cypher query: {}", cypherString);
                    CypherAst ast = CypherAst.parse(cypherString);
                    final Bytecode translation = ast.buildTranslation(Translator.builder().bytecode().enableCypherExtensions().build());
                    // Ensure we reapply any existing options to the translation
                    options.forEach((k, v) -> {
                        if (!k.equals(GafferPopGraphVariables.CYPHER_KEY)) {
                            translation.addSource("with", k, v);
                        }
                    });
                    return translation;
                }
            }
        }
        return bytecode;
    }

    /**
     * Checks the supplied bytecode for restricted steps and strategies.
     *
     * @param bytecode The bytecode to check.
     * @throws AuthorizationException If using restricted features.
     */
    public static void authoriseBytecode(final Bytecode bytecode) throws AuthorizationException {
        final boolean runsLambda = BytecodeHelper.getLambdaLanguage(bytecode).isPresent();
        final boolean touchesReadOnlyStrategy = bytecode.toString().contains(ReadOnlyStrategy.class.getSimpleName());
        final boolean touchesOLAPRestriction = bytecode.toString().contains(VertexProgramRestrictionStrategy.class.getSimpleName());

        final List<String> rejections = new ArrayList<>();
        // Reject use of Lambdas
        if (runsLambda) {
            rejections.add(REJECT_LAMBDA);
        }
        // Reject any modification steps to the graph via gremlin
        if (touchesReadOnlyStrategy) {
            rejections.add(REJECT_MUTATE);
        }
        // Reject use of OLAP operations
        if (touchesOLAPRestriction) {
            rejections.add(REJECT_OLAP);
        }

        // Formulate a rejection message
        String rejectMessage = REJECT_BYTECODE;
        if (!rejections.isEmpty()) {
            rejectMessage += " using " + String.join(", ", rejections);
            throw new AuthorizationException(rejectMessage);
        }
    }

    /**
     * Simple method to help the conversion between {@link BinaryMessage}
     * and {@link ByteBuf} type.
     *
     * @param message the binary web socket message.
     * @return A netty byte buffer for the message.
     */
    public static ByteBuf convertToByteBuf(final BinaryMessage message) {
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
     * Converts plain string Gremlin queries into {@link Bytecode} for further processing.
     *
     * @param query The Gremlin query.
     * @return The bytecode equivalent of the query.
     * @throws ScriptException If issue parsing.
     */
    public static Bytecode convertToBytecode(final String query) throws ScriptException {
        final Bindings scriptBindings = SCRIPT_ENGINE.createBindings();
        scriptBindings.put("g", EMPTY_G);
        CompiledScript compiledScript = SCRIPT_ENGINE.compile(query);

        LOGGER.info("EVAL: {}", compiledScript.eval(scriptBindings));
        return ((DefaultGraphTraversal) compiledScript.eval(scriptBindings)).getBytecode();
        //LOGGER.info("EVAL: {}",  SCRIPT_ENGINE.eval(query, scriptBindings));

        //return ((DefaultGraphTraversal) SCRIPT_ENGINE.eval(query, scriptBindings)).getBytecode();
    }

}
