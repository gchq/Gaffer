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

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.String.format;

public class ArgumentParser {
    public OperationChain parseOperationChain(final String path) {
        final Operation operation = parse(path, Operation.class);
        return operation instanceof OperationChain ? OperationChain.class.cast(operation) : OperationChain.wrap(operation);
    }

    public User parseUser(final String path) {
        return parse(path, User.class);
    }

    private <T> T parse(final String pathArgument, final Class<T> clazz) {
        try {
            final Path path = Paths.get(pathArgument);
            if (Files.isRegularFile(path)) {
                try (InputStream inputStream = Files.newInputStream(path)) {
                    return JSONSerialiser.deserialise(inputStream, clazz);
                }
            } else {
                throw new IllegalArgumentException(format("The path argument: %s could not be read as a file.", pathArgument));
            }
        } catch (final IOException exception) {
            throw new IllegalArgumentException(format("Unable to convert file contents: %s as class: %s", pathArgument, clazz), exception);
        }
    }
}
