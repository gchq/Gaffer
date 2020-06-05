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

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.runner.arguments.ArgumentParser;
import uk.gov.gchq.gaffer.operation.runner.arguments.ArgumentValidator;
import uk.gov.gchq.gaffer.operation.runner.arguments.Arguments;
import uk.gov.gchq.gaffer.operation.runner.arguments.Arguments.Argument;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static uk.gov.gchq.gaffer.operation.runner.arguments.Arguments.Argument.Requirement.MANDATORY;
import static uk.gov.gchq.gaffer.operation.runner.arguments.Arguments.Argument.Requirement.OPTIONAL;
import static uk.gov.gchq.gaffer.store.Context.COMMAND_LINE_ARGS_CONFIG_KEY;

public class OperationRunner {
    private final ArgumentValidator argumentValidator = new ArgumentValidator();
    private final ArgumentParser argumentParser = new ArgumentParser();
    private String schemaPath;
    private String storePropertiesPath;
    private String graphId;
    private OperationChain operationChain;
    private Context context;

    public static void main(final String[] args) {
        run(new OperationRunner(), args);
    }

    static void run(final OperationRunner operationRunner, final String[] args) {
        final ArgumentValidator argumentValidator = operationRunner.argumentValidator;
        final ArgumentParser argumentParser = operationRunner.argumentParser;

        final Argument<OperationChain> operationChainArgument = new Argument<>(
                MANDATORY,
                new String[]{"--operation-chain"},
                argumentValidator::isValidFile,
                argumentParser::parseOperationChain,
                "Path to file containing JSON serialised Operation.");

        final Argument<String> storePropertiesArgument = new Argument<>(
                MANDATORY,
                new String[]{"--store-properties"},
                argumentValidator::isValidFile,
                identity(),
                "Path to file containing store properties.");

        final Argument<String> schemaPathArgument = new Argument<>(
                MANDATORY,
                new String[]{"--schema", "--schema-path"},
                argumentValidator::isValidFileOrDirectory,
                identity(),
                "Path to file or parent directory containing graph schema.");

        final Argument<User> userArgument = new Argument<>(
                OPTIONAL,
                new String[]{"--user"},
                argumentValidator::isValidFile,
                argumentParser::parseUser,
                "Path to file containing JSON serialised User");

        final Argument<String> graphIdArgument = new Argument<>(
                MANDATORY,
                new String[]{"--graph-id"},
                string -> true,
                identity(),
                "The graph Id.");

        final Argument[] argumentDefinitions = new Argument[]{
                operationChainArgument, storePropertiesArgument, schemaPathArgument, userArgument, graphIdArgument
        };
        final Arguments arguments = new Arguments(argumentDefinitions);

        try {
            final Map<Argument, Object> parsedArguments = arguments.parse(args);

            operationRunner.schemaPath = (String) parsedArguments.get(schemaPathArgument);
            operationRunner.storePropertiesPath = (String) parsedArguments.get(storePropertiesArgument);
            operationRunner.graphId = (String) parsedArguments.get(graphIdArgument);
            operationRunner.operationChain = (OperationChain) parsedArguments.get(operationChainArgument);

            final User user = (parsedArguments.containsKey(userArgument))
                    ? (User) parsedArguments.get(userArgument)
                    : new User();

            final Context context = new Context(user);
            context.setConfig(COMMAND_LINE_ARGS_CONFIG_KEY, stripRedundantArgs(args));
            operationRunner.context = context;

            displayOperationResult(operationRunner.run());

        } catch (final IllegalArgumentException e) {
            display(arguments.toDisplayString());

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String[] stripRedundantArgs(final String[] args) {
        return Stream.of(args).filter(OperationRunner::isRequiredArg).collect(toList()).toArray(new String[]{});
    }

    private static boolean isRequiredArg(final String arg) {
        return !OperationRunner.class.getName().equals(arg);
    }

    private static void displayOperationResult(final Object result) {
        if (result instanceof Iterable) {
            display("Results:");
            for (final Object value : (Iterable) result) {
                display(value);
            }
        } else {
            display("Result:");
            display(result);
        }
    }

    private static void display(final Object value) {
        System.out.println(value);
    }

    private Object run() throws OperationException {
        final Graph.Builder graphBuilder = createGraphBuilder()
                .storeProperties(storePropertiesPath)
                .addSchemas(Paths.get(schemaPath))
                .config(new GraphConfig.Builder()
                        .graphId(graphId)
                        .build());
        return execute(graphBuilder);
    }

    protected Object execute(final Graph.Builder graphBuilder) throws OperationException {
        return graphBuilder.build().execute(operationChain, context);
    }

    protected Graph.Builder createGraphBuilder() {
        return new Graph.Builder();
    }

    OperationChain getOperationChain() {
        return operationChain;
    }

    Context getContext() {
        return context;
    }
}
