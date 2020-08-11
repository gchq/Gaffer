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
package uk.gov.gchq.gaffer.operation.runner.arguments;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.runner.arguments.Arguments.Argument;

import java.util.Map;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.operation.runner.arguments.Arguments.Argument.Requirement.Mandatory;
import static uk.gov.gchq.gaffer.operation.runner.arguments.Arguments.Argument.Requirement.Optional;

public class ArgumentsTest {

    @Test
    public void shouldThrowExceptionWhenDuplicateArgumentOptionsSupplied() {
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> new Arguments(new Argument(Mandatory, new String[]{"-a", "-b"}, string -> true, identity(), "An argument"),
                        new Argument(Mandatory, new String[]{"-b", "-c"}, string -> true, identity(), "An argument")));
        assertEquals(String.format("Duplicate option %s configured.", "-b"), actual.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenNotAllMandatoryArgumentsSupplied() {
        shouldThrowExceptionWhenMandatoryArgumentsMissing(
                new String[]{"-a", "a"},
                new Argument(Mandatory, new String[]{"-a", "-b"}, string -> true, identity(), "An argument"),
                new Argument(Mandatory, new String[]{"-c", "-d"}, string -> true, identity(), "An argument"));
    }

    @Test
    public void shouldThrowExceptionWhenNotAllMandatoryArgumentsSupplied2() {
        shouldThrowExceptionWhenMandatoryArgumentsMissing(
                new String[]{"-a", "a", "-c", "c"},
                new Argument(Mandatory, new String[]{"-a", "-b"}, string -> true, identity(), "An argument"),
                new Argument(Mandatory, new String[]{"-c", "-d"}, string -> true, identity(), "An argument"),
                new Argument(Mandatory, new String[]{"-e", "-f"}, string -> true, identity(), "An argument"));
    }

    private void shouldThrowExceptionWhenMandatoryArgumentsMissing(final String[] args, final Argument... arguments) {
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> new Arguments(arguments).parse(args));
        assertEquals("Not all mandatory arguments have been supplied.", actual.getMessage());
    }

    @Test
    public void shouldParseMandatoryArguments() {
        final Argument argumentA = new Argument(Mandatory, new String[]{"-a"}, string -> true, identity(), "An argument");
        final Argument argumentB = new Argument(Mandatory, new String[]{"-b"}, string -> true, identity(), "An argument");

        final Argument[] arguments = new Argument[]{argumentA, argumentB};

        final Map<Argument, Object> parsedArguments = new Arguments(arguments).parse(new String[]{"-a", "a", "-b", "b"});
        assertEquals(arguments.length, parsedArguments.size());
        assertTrue(parsedArguments.containsKey(argumentA));
        assertEquals("a", parsedArguments.get(argumentA));
        assertTrue(parsedArguments.containsKey(argumentB));
        assertEquals("b", parsedArguments.get(argumentB));
    }

    @Test
    public void shouldParseArgumentsNotIncludingOptional() {
        final Argument argumentA = new Argument(Mandatory, new String[]{"-a"}, string -> true, identity(), "An argument");
        final Argument argumentB = new Argument(Mandatory, new String[]{"-b"}, string -> true, identity(), "An argument");
        final Argument argumentC = new Argument(Optional, new String[]{"-c"}, string -> true, identity(), "An argument");

        final Argument[] arguments = new Argument[]{argumentA, argumentB, argumentC};

        final Map<Argument, Object> parsedArguments = new Arguments(arguments).parse(new String[]{"-a", "a", "-b", "b"});
        assertEquals(2, parsedArguments.size());
        assertTrue(parsedArguments.containsKey(argumentA));
        assertEquals("a", parsedArguments.get(argumentA));
        assertTrue(parsedArguments.containsKey(argumentB));
        assertEquals("b", parsedArguments.get(argumentB));
    }

    @Test
    public void shouldParseArgumentsIncludingOptional() {
        final Argument argumentA = new Argument(Mandatory, new String[]{"-a"}, string -> true, identity(), "An argument");
        final Argument argumentB = new Argument(Mandatory, new String[]{"-b"}, string -> true, identity(), "An argument");
        final Argument argumentC = new Argument(Optional, new String[]{"-c"}, string -> true, identity(), "An argument");

        final Argument[] arguments = new Argument[]{argumentA, argumentB, argumentC};

        final Map<Argument, Object> parsedArguments = new Arguments(arguments).parse(new String[]{"-a", "a", "-b", "b", "-c", "c"});
        assertEquals(arguments.length, parsedArguments.size());
        assertTrue(parsedArguments.containsKey(argumentA));
        assertEquals("a", parsedArguments.get(argumentA));
        assertTrue(parsedArguments.containsKey(argumentB));
        assertEquals("b", parsedArguments.get(argumentB));
        assertTrue(parsedArguments.containsKey(argumentC));
        assertEquals("c", parsedArguments.get(argumentC));
    }

    @Test
    public void shouldIgnoreUnknownArguments() {
        final Argument argumentA = new Argument(Mandatory, new String[]{"-a"}, string -> true, identity(), "An argument");
        final Argument argumentB = new Argument(Mandatory, new String[]{"-b"}, string -> true, identity(), "An argument");

        final Argument[] arguments = new Argument[]{argumentA, argumentB};

        final Map<Argument, Object> parsedArguments = new Arguments(arguments).parse(new String[]{"jar", "class", "-libjars", "libjar1,libjar2", "-a", "a", "-x", "-z", "something", "-b", "b", "-c", "c"});
        assertEquals(arguments.length, parsedArguments.size());
        assertTrue(parsedArguments.containsKey(argumentA));
        assertEquals("a", parsedArguments.get(argumentA));
        assertTrue(parsedArguments.containsKey(argumentB));
        assertEquals("b", parsedArguments.get(argumentB));
    }

    @Test
    public void shouldDisplayArgumentInformation() {
        final Argument argumentA = new Argument(Mandatory, new String[]{"-a", "--a"}, string -> true, identity(), "Mandatory argument A");
        assertEquals("[-a, --a] (Mandatory)\n- Mandatory argument A", argumentA.toDisplayString());

        final Argument argumentB = new Argument(Optional, new String[]{"-b"}, string -> true, identity(), "Optional argument B");
        assertEquals("[-b] (Optional)\n- Optional argument B", argumentB.toDisplayString());
    }

    @Test
    public void shouldDisplayArgumentsInformation() {
        final Argument argumentA = new Argument(Mandatory, new String[]{"-a", "--a"}, string -> true, identity(), "Mandatory argument A");
        final Argument argumentB = new Argument(Optional, new String[]{"-b"}, string -> true, identity(), "Optional argument B");

        final Arguments arguments = new Arguments(argumentA, argumentB);

        assertEquals("Usage:\n[-a, --a] (Mandatory)\n- Mandatory argument A\n[-b] (Optional)\n- Optional argument B", arguments.toDisplayString());
    }
}
