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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * A {@code AddElementsFromSocketHandler} handles the {@link AddElementsFromSocket}
 * operation.
 *
 * This uses Flink to stream the {@link uk.gov.gchq.gaffer.data.element.Element}
 * objects from a socket into Gaffer.
 */
public class AddElementsFromSocketHandler implements OperationHandler<AddElementsFromSocket> {
    @Override
    public Object doOperation(final AddElementsFromSocket op, final Context context, final Store store) throws OperationException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (null != op.getParallelism()) {
            env.setParallelism(op.getParallelism());
        }

        env.socketTextStream(op.getHostname(), op.getPort(), op.getDelimiter())
                .map(new GafferMapFunction(op.getElementGenerator()))
                .returns(GafferMapFunction.RETURN_CLASS)
                .rebalance()
                .addSink(new GafferSink(op, store));


        try {
            env.execute(op.getClass().getSimpleName() + "-" + op.getHostname() + ":" + op.getPort());
        } catch (final Exception e) {
            throw new OperationException("Failed to add elements from port: " + op.getPort(), e);
        }

        return null;
    }
}
