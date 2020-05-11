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
package uk.gov.gchq.gaffer.operation.runner.argument.converter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.function.BiFunction;

import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ArgumentConverterTest {
    private static final String NON_EXISTENT_PATH = "/a/non/existent/path";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final ArgumentConverter argumentConverter = new ArgumentConverter();

    @Test
    public void shouldThrowExceptionIfPathDoesNotExist() {
        shouldThrowExceptionIfPathDoesNotExist(NON_EXISTENT_PATH, User.class, argumentConverter::convert);
    }

    @Test
    public void shouldThrowExceptionIfUserPathIsNotFile() {
        shouldThrowExceptionIfPathDoesNotExist(temporaryFolder.getRoot().getPath(), User.class, argumentConverter::convert);
    }

    @Test
    public void shouldThrowExceptionIfUserPathContainsInvalidContent() throws IOException {
        shouldThrowExceptionIfPathContainsInvalidContent(createFileContaining("junk").getPath(), User.class, argumentConverter::convert);
    }

    @Test
    public void shouldParseValidUserContent() throws IOException {
        final User user = new User.Builder()
                .userId("userId")
                .dataAuth("dataAuth")
                .opAuth("opAuth")
                .build();

        final User parsedUser = argumentConverter.convert(createFileContaining(JSONSerialiser.serialise(user)).getPath(), User.class);

        assertEquals(user, parsedUser);
    }

    @Test
    public void shouldParseValidOperationContent() throws IOException {
        final GetAllElements operation = new GetAllElements.Builder()
                .directedType(DirectedType.EITHER)
                .build();

        final Operation parsedOperation = argumentConverter.convert(createFileContaining(JSONSerialiser.serialise(operation)).getPath(), Operation.class);

        assertArrayEquals(JSONSerialiser.serialise(operation), JSONSerialiser.serialise(parsedOperation));
    }

    private File createFileContaining(final String content) throws IOException {
        return createFileContaining(content.getBytes());
    }

    private File createFileContaining(final byte[] content) throws IOException {
        return createFileContaining(new ByteArrayInputStream(content));
    }

    private File createFileContaining(final InputStream contentStream) throws IOException {
        final File contentFile = temporaryFolder.newFile();
        Files.copy(contentStream, contentFile.toPath(), REPLACE_EXISTING);
        return contentFile;
    }

    private <T> void shouldThrowExceptionIfPathDoesNotExist(final String path, final Class<T> clazz, final BiFunction<String, Class<T>, T> convertFunction) {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(format("The path argument: [%s] could not be read as a file.", path));
        convertFunction.apply(path, clazz);
    }

    private <T> void shouldThrowExceptionIfPathContainsInvalidContent(final String path, final Class<T> clazz, final BiFunction<String, Class<T>, T> convertFunction) {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(format("Unable to convert file contents: [%s] as class: [%s]", path, clazz));
        convertFunction.apply(path, clazz);
    }
}
