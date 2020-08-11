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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArgumentValidatorTest {
    private static final String NON_EXISTENT_PATH = "/not/existent/path";

    private final ArgumentValidator argumentValidator = new ArgumentValidator();

    @Test
    public void shouldReturnFalseWhenPathIsNotFile() {
        assertFalse(argumentValidator.isValidFile(NON_EXISTENT_PATH));
    }

    @Test
    public void shouldReturnFalseWhenPathIsNotDirectory() {
        assertFalse(argumentValidator.isValidDirectory(NON_EXISTENT_PATH));
    }

    @Test
    public void shouldReturnFalseWhenPathIsNotFileOrDirectory() {
        assertFalse(argumentValidator.isValidFileOrDirectory(NON_EXISTENT_PATH));
    }

    @Test
    public void shouldReturnTrueWhenPathIsValidFile(@TempDir Path tempDir) throws IOException {
        assertTrue(argumentValidator.isValidFile(Files.createTempFile(tempDir, null, null).toAbsolutePath().toString()));
    }

    @Test
    public void shouldReturnTrueWhenPathIsValidDirectory(@TempDir Path tempDir) {
        assertTrue(argumentValidator.isValidDirectory(tempDir.toAbsolutePath().toString()));
    }

    @Test
    public void shouldReturnTrueWhenPathIsValidFileOrDirectory(@TempDir Path tempDir) throws IOException {
        assertTrue(argumentValidator.isValidFile(Files.createTempFile(tempDir, null, null).toAbsolutePath().toString()));
        assertTrue(argumentValidator.isValidDirectory(tempDir.toAbsolutePath().toString()));
    }
}
