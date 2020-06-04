/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.runner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Console;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.runner.argument.converter.OperationChainConverter;
import uk.gov.gchq.gaffer.operation.runner.argument.converter.UserConverter;
import uk.gov.gchq.gaffer.operation.runner.argument.validator.IsFileOrDirectoryValidator;
import uk.gov.gchq.gaffer.operation.runner.argument.validator.IsFileValidator;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.nio.file.Paths;

import static java.lang.String.format;
import static uk.gov.gchq.gaffer.store.Context.COMMAND_LINE_ARGS_CONFIG_KEY;

public class OperationRunner {
    @Parameter(names = "--help", description = "Display usage.", help = true)
    private boolean help;

    @Parameter(names = {"--op", "--operation-chain"}, description = "Path to the JSON serialised Operation to execute.", required = true, converter = OperationChainConverter.class, validateWith = IsFileValidator.class)
    private OperationChain operationChain;

    @Parameter(names = {"--sp", "--store-properties"}, description = "Path to the store properties.", required = true, validateWith = IsFileValidator.class)
    private String storeProperties;

    @Parameter(names = {"--schema", "--schema-path"}, description = "Path to the schema definition.", required = true, validateWith = IsFileOrDirectoryValidator.class)
    private String schemaPath;

    @Parameter(names = {"--u", "--user"}, description = "Path to the JSON serialised User to execute the Operation as.", converter = UserConverter.class, validateWith = IsFileValidator.class)
    private User user = new User();

    @Parameter(names = {"--g", "--graph-id"}, description = "GraphId", required = true)
    private String graphId;

    private static String[] args;

    public static void main(final String[] args) {
        OperationRunner.args = args;
        run(new OperationRunner(), args);
    }

    static void run(final OperationRunner operationRunner, final String[] args) {
        final JCommander jcommander = new JCommander();
        jcommander.addObject(operationRunner);
        jcommander.setProgramName(OperationRunner.class.getName());
        jcommander.setAcceptUnknownOptions(true);

        try {
            jcommander.parse(args);
            if (operationRunner.help) {
                jcommander.usage();
            } else {
                display(operationRunner.run(), jcommander.getConsole());
            }
        } catch (final Exception exception) {
            jcommander.getConsole().println(format("Unable to process request, received exception: %s", exception.getMessage()));
            jcommander.usage();
            exception.printStackTrace();
        }
    }

    private Object run() throws OperationException {
        final Graph.Builder graphBuilder = createGraphBuilder()
                .storeProperties(storeProperties)
                .addSchemas(Paths.get(schemaPath))
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .build());
        return execute(graphBuilder);
    }

    protected Graph.Builder createGraphBuilder() {
        return new Graph.Builder();
    }

    protected Object execute(final Graph.Builder graphBuilder) throws OperationException {
        final Context context = new Context(user);
        context.setConfig(COMMAND_LINE_ARGS_CONFIG_KEY, args);
        return graphBuilder.build().execute(operationChain, context);
    }

    private static void display(final Object result, final Console console) {
        if (result instanceof Iterable) {
            console.println("Results:");
            for (final Object value : (Iterable) result) {
                console.println(toString(value));
            }
        } else {
            console.println("Result:");
            console.println(toString(result));
        }
    }

    private static String toString(final Object object) {
        return object == null ? null : object.toString();
    }

    User getUser() {
        return user;
    }

    OperationChain getOperationChain() {
        return operationChain;
    }
}
