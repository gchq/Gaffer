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

import com.beust.jcommander.IStringConverter;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;

public class OperationChainConverter implements IStringConverter<OperationChain> {
    private final ArgumentConverter argumentConverter;

    public OperationChainConverter() {
        this.argumentConverter = new ArgumentConverter();
    }

    OperationChainConverter(final ArgumentConverter argumentConverter) {
        this.argumentConverter = argumentConverter;
    }

    @Override
    public OperationChain convert(final String value) {
        final Operation operation = argumentConverter.convert(value, Operation.class);
        return operation instanceof OperationChain ? OperationChain.class.cast(operation) : OperationChain.wrap(operation);
    }
}
