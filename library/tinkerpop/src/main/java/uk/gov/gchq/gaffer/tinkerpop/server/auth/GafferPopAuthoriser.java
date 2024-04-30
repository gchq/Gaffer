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

package uk.gov.gchq.gaffer.tinkerpop.server.auth;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode.Instruction;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;

public class GafferPopAuthoriser implements Authorizer {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopAuthoriser.class);
    public static final String REJECT_BYTECODE = "User not authorized for bytecode requests on %s";
    public static final String REJECT_LAMBDA = "lambdas";
    public static final String REJECT_MUTATE = "the ReadOnlyStrategy";
    public static final String REJECT_OLAP = "the VertexProgramRestrictionStrategy";
    public static final String REJECT_STRING = "User not authorized for string-based requests.";

    /**
     * This method is called once upon system startup to initialize the
     * {@code GafferAuthoriser}.
     */
    @Override
    public void setup(final Map<String,Object> config) {
        // Nothing to setup
    }

    /**
     * Checks whether a user is authorized to have a gremlin bytecode request from a client answered and raises an
     * {@link AuthorizationException} if this is not the case. If authorised will modify the bytecode to inject the
     * users details via a 'with()' step so the Gaffer graph can run with the correct user.
     *
     * @param user {@link AuthenticatedUser} that needs authorization.
     * @param bytecode The gremlin {@link Bytecode} request to authorize the user for.
     * @param aliases A {@link Map} with a single key/value pair that maps the name of the {@link TraversalSource} in the
     *                {@link Bytecode} request to name of one configured in Gremlin Server.
     * @return The original or modified {@link Bytecode} to be used for further processing.
     */
    @Override
    public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases) throws AuthorizationException {
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
            throw new AuthorizationException(String.format(rejectMessage, aliases.values()));
        }

        // Prevent overriding the user ID in a 'with()' block as we will set it based on the authenticated user
        for (Instruction i : bytecode.getStepInstructions()) {
            LOGGER.info("OPERATOR {}, ARGS {}", i.getOperator(), i.getArguments());
            if (i.getOperator().equals("with") && Arrays.asList(i.getArguments()).contains(GafferPopGraphVariables.USER_ID)) {
                throw new AuthorizationException("Can't override current user ID from within a query");
            }
        }

        // Add the user ID to the query
        bytecode.addSource("with", GafferPopGraphVariables.USER_ID, user.getName());

        return bytecode;
    }

    /**
     * Checks whether a user is authorized to have a script request from a gremlin
     * client answered and raises an
     * {@link AuthorizationException} if this is not the case.
     *
     * @param user {@link AuthenticatedUser} that needs authorization.
     * @param msg  {@link RequestMessage} in which the
     *             {@link org.apache.tinkerpop.gremlin.driver.Tokens}.ARGS_GREMLIN
     *             argument can contain an arbitrary succession of script
     *             statements.
     */
    @Override
    public void authorize(AuthenticatedUser user, RequestMessage msg) throws AuthorizationException {
        // Not supported in GafferPop
        throw new AuthorizationException(REJECT_STRING);
    }

}
