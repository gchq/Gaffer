/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.add;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.generator.TestGeneratorImpl;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class AddElementsFromKafkaTest extends OperationTest<AddElementsFromKafka> {

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final boolean validate = true;
        final boolean skipInvalid = false;
        final Integer parallelism = 2;
        final Class<TestGeneratorImpl> generator = TestGeneratorImpl.class;
        final String groupId = "groupId";
        final String topic = "topic";
        final String[] servers = {"server1", "server2"};
        final AddElementsFromKafka op = new AddElementsFromKafka.Builder()
                .generator(generator)
                .parallelism(parallelism)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .groupId(groupId)
                .topic(topic)
                .bootstrapServers(servers)
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(op, true);
        final AddElementsFromKafka deserialisedOp = JSONSerialiser.deserialise(json, AddElementsFromKafka.class);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                        "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromKafka\",%n" +
                        "  \"topic\" : \"topic\",%n" +
                        "  \"groupId\" : \"groupId\",%n" +
                        "  \"bootstrapServers\" : [ \"server1\", \"server2\" ],%n" +
                        "  \"parallelism\" : 2,%n" +
                        "  \"elementGenerator\" : \"uk.gov.gchq.gaffer.generator.TestGeneratorImpl\"%n" +
                        "}").getBytes(),
                json);
        assertEquals(generator, deserialisedOp.getElementGenerator());
        assertEquals(parallelism, deserialisedOp.getParallelism());
        assertEquals(validate, deserialisedOp.isValidate());
        assertEquals(skipInvalid, deserialisedOp.isSkipInvalidElements());
        assertEquals(groupId, deserialisedOp.getGroupId());
        assertEquals(topic, deserialisedOp.getTopic());
        assertArrayEquals(servers, deserialisedOp.getBootstrapServers());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final boolean validate = true;
        final boolean skipInvalid = false;
        final Integer parallelism = 2;
        final Class<TestGeneratorImpl> generator = TestGeneratorImpl.class;
        final String groupId = "groupId";
        final String topic = "topic";
        final String[] servers = {"server1", "server2"};

        // When
        final AddElementsFromKafka op = new AddElementsFromKafka.Builder()
                .generator(generator)
                .parallelism(parallelism)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .groupId(groupId)
                .topic(topic)
                .bootstrapServers(servers)
                .build();

        // Then
        assertEquals(generator, op.getElementGenerator());
        assertEquals(parallelism, op.getParallelism());
        assertEquals(validate, op.isValidate());
        assertEquals(skipInvalid, op.isSkipInvalidElements());
        assertEquals(groupId, op.getGroupId());
        assertEquals(topic, op.getTopic());
        assertArrayEquals(servers, op.getBootstrapServers());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final boolean validate = true;
        final boolean skipInvalid = false;
        final Integer parallelism = 2;
        final Class<TestGeneratorImpl> generator = TestGeneratorImpl.class;
        final String groupId = "groupId";
        final String topic = "topic";
        final String[] servers = {"server1", "server2"};
        final AddElementsFromKafka addElementsFromKafka = new AddElementsFromKafka.Builder()
                .generator(generator)
                .parallelism(parallelism)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .groupId(groupId)
                .topic(topic)
                .bootstrapServers(servers)
                .option("testOption", "true")
                .build();

        // When
        final AddElementsFromKafka clone = addElementsFromKafka.shallowClone();

        // Then
        assertNotSame(addElementsFromKafka, clone);
        assertEquals(validate, clone.isValidate());
        assertEquals(skipInvalid, clone.isSkipInvalidElements());
        assertEquals(parallelism, clone.getParallelism());
        assertEquals(generator, clone.getElementGenerator());
        assertEquals(groupId, clone.getGroupId());
        assertEquals(topic, clone.getTopic());
        assertArrayEquals(servers, clone.getBootstrapServers());
        assertEquals("true", clone.getOption("testOption"));
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("topic", "groupId", "bootstrapServers", "elementGenerator");
    }

    @Override
    protected AddElementsFromKafka getTestObject() {
        return new AddElementsFromKafka();
    }
}
