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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;

public class Arguments {
    private final Argument[] arguments;
    private final Map<String, Argument> argumentMap;

    public Arguments(final Argument... arguments) {
        this.arguments = arguments;
        this.argumentMap = new HashMap<>(arguments.length);
        for (final Argument argument : arguments) {
            for (final String option : argument.options) {
                if (argumentMap.containsKey(option)) {
                    throw new IllegalArgumentException(format("Duplicate option %s configured.", option));
                } else {
                    argumentMap.put(option, argument);
                }
            }
        }
    }

    public Map<Argument, Object> parse(final String[] args) {
        final Map<Argument, Object> parsedArguments = new HashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            final String option = args[i];
            final Optional<Argument> optionalArgument = getArgumentForOption(option);
            if (optionalArgument.isPresent()) {
                final Argument argument = optionalArgument.get();
                final String optionValue = args[++i];
                if (argument.isValid(optionValue)) {
                    parsedArguments.put(argument, argument.convert(optionValue));
                } else {
                    throw new IllegalArgumentException(format("Value: %s supplied for option: %s is not valid.", optionValue, option));
                }
            }
        }

        ensureMandatoryArgumentsPresent(parsedArguments);

        return parsedArguments;
    }

    public String toDisplayString() {
        return new StringBuilder("Usage:")
                .append(lineSeparator())
                .append(Arrays.stream(arguments).map(Argument::toDisplayString).collect(joining(lineSeparator())))
                .toString();
    }

    private void ensureMandatoryArgumentsPresent(final Map<Argument, Object> parsedArguments) {
        final Set<Argument> mandatoryArguments = Stream.of(arguments).filter(Argument::isMandatory).collect(Collectors.toSet());
        mandatoryArguments.removeAll(parsedArguments.keySet());
        if (!mandatoryArguments.isEmpty()) {
            throw new IllegalArgumentException("Not all mandatory arguments have been supplied.");
        }
    }

    public Optional<Argument> getArgumentForOption(final String option) {
        if (argumentMap.containsKey(option)) {
            return Optional.of(argumentMap.get(option));
        }
        return Optional.empty();
    }

    public static class Argument<T> {
        public enum Requirement {
            Mandatory, Optional;
        }

        private final Requirement requirement;
        private final String[] options;
        private final Predicate<String> validator;
        private final Function<String, T> converter;
        private final String description;

        public Argument(
                final Requirement requirement,
                final String[] options,
                final Predicate<String> validator,
                final Function<String, T> converter,
                final String description) {
            this.requirement = requirement;
            this.options = options;
            this.validator = validator;
            this.converter = converter;
            this.description = description;
        }

        public boolean isValid(final String value) {
            return validator.test(value);
        }

        public T convert(final String value) {
            return converter.apply(value);
        }

        public boolean isMandatory() {
            return requirement == Requirement.Mandatory;
        }

        public String toDisplayString() {
            return new StringBuilder(Arrays.deepToString(options))
                    .append(" (")
                    .append(requirement)
                    .append(")")
                    .append(lineSeparator())
                    .append("- ")
                    .append(description)
                    .toString();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Argument<?> argument = (Argument<?>) o;
            return requirement == argument.requirement &&
                    Arrays.equals(options, argument.options) &&
                    validator.equals(argument.validator) &&
                    converter.equals(argument.converter) &&
                    description.equals(argument.description);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(requirement, validator, converter, description);
            result = 31 * result + Arrays.hashCode(options);
            return result;
        }
    }
}
