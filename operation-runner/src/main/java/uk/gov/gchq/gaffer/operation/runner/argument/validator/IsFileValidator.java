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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.nio.file.Files;
import java.nio.file.Paths;

import static java.lang.String.format;

public class IsFileValidator implements IParameterValidator {
    @Override
    public void validate(final String name, final String value) throws ParameterException {
        if (!Files.isRegularFile(Paths.get(value))) {
            throw new ParameterException(format("The value: [%s] for parameter: [%s] is not a valid file.", value, name));
        }
    }
}
