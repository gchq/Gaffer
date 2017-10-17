/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.flink.operation.handler;

import org.apache.flink.api.java.ExecutionEnvironment;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * A {@code AddElementsFromFileHandler} handles the {@link AddElementsFromFile}
 * operation.
 *
 * This uses Flink to stream the {@link uk.gov.gchq.gaffer.data.element.Element}
 * objects from a file into Gaffer.
 */
public class AddElementsFromFileHandler implements OperationHandler<AddElementsFromFile> {
    @Override
    public Object doOperation(final AddElementsFromFile op, final Context context, final Store store) throws OperationException {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        if (null != op.getParallelism()) {
            env.setParallelism(op.getParallelism());
        }

        env.readTextFile(op.getFilename())
                .map(new GafferMapFunction(op.getElementGenerator()))
                .returns(GafferMapFunction.RETURN_CLASS)
                .rebalance()
                .output(new GafferOutput(op, store));

        try {
            env.execute(op.getClass().getSimpleName() + "-" + op.getFilename());
        } catch (final Exception e) {
            throw new OperationException("Failed to add elements from file: " + op.getFilename(), e);
        }

        return null;
    }
}
