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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.function.Function;

import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArgumentParserTest {
    private static final String NON_EXISTENT_PATH = "/not/existent/path";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final ArgumentParser argumentParser = new ArgumentParser();

    @Test
    public void shouldThrowExceptionIfUserPathDoesNotExist() {
        shouldThrowExceptionIfPathDoesNotExist(NON_EXISTENT_PATH, argumentParser::parseUser);
    }

    @Test
    public void shouldThrowExceptionIfOperationChainPathDoesNotExist() {
        shouldThrowExceptionIfPathDoesNotExist(NON_EXISTENT_PATH, argumentParser::parseOperationChain);
    }

    @Test
    public void shouldThrowExceptionIfUserPathIsNotFile() {
        shouldThrowExceptionIfPathDoesNotExist(temporaryFolder.getRoot().getPath(), argumentParser::parseUser);
    }

    @Test
    public void shouldThrowExceptionIfOperationChainPathIsNotFile() {
        shouldThrowExceptionIfPathDoesNotExist(temporaryFolder.getRoot().getPath(), argumentParser::parseOperationChain);
    }

    @Test
    public void shouldThrowExceptionIfUserPathContainsInvalidContent() throws IOException {
        shouldThrowExceptionIfPathContainsInvalidContent(createFileContaining("junk").getPath(), User.class, argumentParser::parseUser);
    }

    @Test
    public void shouldThrowExceptionIfOperationChainPathContainsInvalidContent() throws IOException {
        shouldThrowExceptionIfPathContainsInvalidContent(createFileContaining("junk").getPath(), Operation.class, argumentParser::parseOperationChain);
    }

    @Test
    public void shouldParseValidUserContent() throws IOException {
        final User user = new User.Builder()
                .userId("userId")
                .dataAuth("dataAuth")
                .opAuth("opAuth")
                .build();

        final User parsedUser = argumentParser.parseUser(createFileContaining(JSONSerialiser.serialise(user)).getPath());

        assertEquals(user.getUserId(), parsedUser.getUserId());
        assertEquals(user.getDataAuths(), parsedUser.getDataAuths());
        assertEquals(user.getOpAuths(), parsedUser.getOpAuths());
    }

    @Test
    public void shouldParseValidOperationContent() throws IOException {
        final GetAllElements operation = new GetAllElements.Builder()
                .directedType(DirectedType.EITHER)
                .build();

        final OperationChain parsedOperationChain = argumentParser.parseOperationChain(createFileContaining(JSONSerialiser.serialise(operation)).getPath());

        assertEquals(1, parsedOperationChain.getOperations().size());
        final Operation parsedOperation = (Operation)parsedOperationChain.getOperations().get(0);
        assertTrue(parsedOperation instanceof GetAllElements);
        final GetAllElements parsedGetAllElements = GetAllElements.class.cast(parsedOperation);
        assertEquals(operation.getDirectedType(), parsedGetAllElements.getDirectedType());
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

    private <T> void shouldThrowExceptionIfPathDoesNotExist(final String path, final Function<String, T> parseFunction) {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(format("The path argument: %s could not be read as a file.", path));
        parseFunction.apply(path);
    }

    private <T> void shouldThrowExceptionIfPathContainsInvalidContent(final String path, final Class<?> clazz, final Function<String, T> parseFunction) {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(format("Unable to convert file contents: %s as class: %s", path, clazz));
        parseFunction.apply(path);
    }
}
