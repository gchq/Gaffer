/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.jobtracker;

import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class JobDetailTest {

    @Test
    public void shouldBeSerialisable() throws SerialisationException {
        // given
        JobDetail original = new JobDetail.Builder()
                .description("thing")
                .jobId("abc")
                .parentJobId("cde")
                .repeat(new Repeat(20L, 30L, TimeUnit.MINUTES))
                .status(JobStatus.RUNNING)
                .userId("a user")
                .opChain(new OperationChain.Builder().first(new GetAllElements()).build())
                .build();

        final JavaSerialiser serialiser = new JavaSerialiser();

        // when
        final byte[] serialised = serialiser.serialise(original);

        // then
        assertEquals(original, serialiser.deserialise(serialised));
    }
}
