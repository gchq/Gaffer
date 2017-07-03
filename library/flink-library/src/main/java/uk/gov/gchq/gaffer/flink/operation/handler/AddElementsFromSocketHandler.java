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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import uk.gov.gchq.gaffer.flink.operation.AddElementsFromSocket;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.Map;

public class AddElementsFromSocketHandler implements OperationHandler<AddElementsFromSocket> {
    @Override
    public Object doOperation(final AddElementsFromSocket operation, final Context context, final Store store) throws OperationException {

        validateOperation(operation);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream(operation.getHostname(), operation.getPort(), "\n")
                .map(new GafferMapFunction(operation.getElementGenerator()))
                .returns(GafferMapFunction.getReturnClass())
                .rebalance()
                .addSink(new GafferSink(store));

        try {
            env.execute(operation.getJobName());
        } catch (final Exception e) {
            throw new OperationException("Failed to add elements from port: " + operation.getPort(), e);
        }

        return null;
    }

    private Map<String, String> validateOperation(final AddElementsFromSocket operation) {

        final Map<String, String> options = operation.getOptions();

        if (null == operation.getHostname() || StringUtils.isEmpty(operation.getHostname())) {
            throw new IllegalArgumentException("Unable to build AddElementsFromSocket operation - operation.hostname not set");
        }
        if (null == (Integer) operation.getPort()) {
            throw new IllegalArgumentException("Unable to build AddElementsFromSocket operation - operation.port not set");
        }

        if (operation.getJobName() == null || StringUtils.isEmpty(operation.getJobName())) {
            throw new IllegalArgumentException("Unable to build AddElementsFromSocket operation - operation.jobName is not set");
        }
        return options;
    }
}
