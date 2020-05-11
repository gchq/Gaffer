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
package uk.gov.gchq.gaffer.operation.runner.argument.validator;

import com.beust.jcommander.ParameterException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static java.lang.String.format;

public class IsFileValidatorTest {
    private static final String PARAMETER = "--param";
    private static final String NON_EXISTENT_PATH = "/a/non/existent/path";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final IsFileValidator isFileValidator = new IsFileValidator();

    @Test
    public void shouldThrowExceptionWhenPathIsNotFile() {
        expectedException.expect(ParameterException.class);
        expectedException.expectMessage(format("The value: [%s] for parameter: [%s] is not a valid file.", NON_EXISTENT_PATH, PARAMETER));
        isFileValidator.validate(PARAMETER, NON_EXISTENT_PATH);
    }

    @Test
    public void shouldValidateWhenPathIsValidFile() throws IOException {
        isFileValidator.validate(PARAMETER, temporaryFolder.newFile().getPath());
    }
}
