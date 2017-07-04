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

package uk.gov.gchq.gaffer.flink.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AddElementsFromSocketTest extends OperationTest {
    @Override
    protected Class<? extends Operation> getOperationClass() {
        return AddElementsFromSocket.class;
    }

    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException, JsonProcessingException {
        // Given
        final boolean validate = true;
        final boolean skipInvalid = false;
        final int parallelism = 2;
        final String jobName = "jobName";
        final Class<FlinkTest.BasicGenerator> generator = FlinkTest.BasicGenerator.class;
        final int port = 6874;
        final String hostname = "hostname";
        final String delimiter = ",";
        final AddElementsFromSocket op = new AddElementsFromSocket.Builder()
                .jobName(jobName)
                .generator(generator)
                .parallelism(parallelism)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .hostname(hostname)
                .port(port)
                .delimiter(delimiter)
                .build();

        // When
        final byte[] json = JSON_SERIALISER.serialise(op, true);
        final AddElementsFromSocket deserialisedOp = JSON_SERIALISER.deserialise(json, AddElementsFromSocket.class);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                        "  \"class\" : \"uk.gov.gchq.gaffer.flink.operation.AddElementsFromSocket\",%n" +
                        "  \"hostname\" : \"hostname\",%n" +
                        "  \"port\" : 6874,%n" +
                        "  \"elementGenerator\" : \"uk.gov.gchq.gaffer.flink.operation.FlinkTest$BasicGenerator\",%n" +
                        "  \"jobName\" : \"jobName\",%n" +
                        "  \"parallelism\" : 2,%n" +
                        "  \"validate\" : true,%n" +
                        "  \"skipInvalidElements\" : false,%n" +
                        "  \"delimiter\" : \",\"%n" +
                        "}").getBytes(),
                json);
        assertEquals(jobName, deserialisedOp.getJobName());
        assertEquals(generator, deserialisedOp.getElementGenerator());
        assertEquals(parallelism, deserialisedOp.getParallelism());
        assertEquals(validate, deserialisedOp.isValidate());
        assertEquals(skipInvalid, deserialisedOp.isSkipInvalidElements());
        assertEquals(hostname, deserialisedOp.getHostname());
        assertEquals(port, deserialisedOp.getPort());
        assertEquals(delimiter, deserialisedOp.getDelimiter());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final boolean validate = true;
        final boolean skipInvalid = false;
        final int parallelism = 2;
        final String jobName = "jobName";
        final Class<FlinkTest.BasicGenerator> generator = FlinkTest.BasicGenerator.class;
        final int port = 6874;
        final String hostname = "hostname";
        final String delimiter = ",";

        // Given
        final AddElementsFromSocket op = new AddElementsFromSocket.Builder()
                .jobName(jobName)
                .generator(generator)
                .parallelism(parallelism)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .hostname(hostname)
                .port(port)
                .delimiter(delimiter)
                .build();


        // Then
        assertEquals(jobName, op.getJobName());
        assertEquals(generator, op.getElementGenerator());
        assertEquals(parallelism, op.getParallelism());
        assertEquals(validate, op.isValidate());
        assertEquals(skipInvalid, op.isSkipInvalidElements());
        assertEquals(hostname, op.getHostname());
        assertEquals(port, op.getPort());
        assertEquals(delimiter, op.getDelimiter());
    }

    @Override
    protected Set<String> getRequiredFields() {
        // port is required but as it is an int it cannot be null
        return Sets.newHashSet("hostname", "jobName", "elementGenerator");
    }
}
