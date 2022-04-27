/*
 * Copyright 2020-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

public class RFileReaderIteratorTest {

    @Test
    public void initWithAnyConfigShouldNotHaveNextIterator() {
        final AccumuloTablet partition = new AccumuloTablet(0, 0, "a", "b");
        final TaskContext taskContext = mock(TaskContext.class);
        final Set<String> auths = new HashSet<>();

        final Configuration configuration = new Configuration();
        configuration.set("any key", "blahh");

        final RFileReaderIterator iterator = new RFileReaderIterator(partition, taskContext, configuration, auths);

        assertThat(iterator).isExhausted();
    }

    @Test
    public void initWithEmptyAccumuloTabletDoesNotHaveNextIterator() {
        final Partition partition = new AccumuloTablet(0, 0, "a", "b");
        final TaskContext taskContext = mock(TaskContext.class);
        final Set<String> auths = new HashSet<>();

        final RFileReaderIterator iterator = new RFileReaderIterator(partition, taskContext, new Configuration(), auths);

        assertThat(iterator).isExhausted();
    }

    @Test
    public void initWithNullAuthsDoesNotHaveNextIterator() {
        final Partition partition = new AccumuloTablet(0, 0, "a", "b");
        final TaskContext taskContext = mock(TaskContext.class);

        final RFileReaderIterator iterator = new RFileReaderIterator(partition, taskContext, new Configuration(), null);

        assertThat(iterator).isExhausted();
    }

    @Test
    public void initWithInvalidFileAddedToAccumuloShouldThrowRuntimeException() {
        final AccumuloTablet partition = new AccumuloTablet(0, 0, "a", "b");
        final TaskContext taskContext = mock(TaskContext.class);
        final Set<String> auths = new HashSet<>();

        partition.addRFile("invalid file");

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> new RFileReaderIterator(partition, taskContext, new Configuration(), auths))
                .withMessage("IOException initialising RFileReaderIterator");
    }

    @Test
    public void initWithNullTaskContentShouldThrowNPE() {
        final Partition partition = new AccumuloTablet(0, 0, "a", "b");
        final Set<String> auths = new HashSet<>();

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new RFileReaderIterator(partition, null, new Configuration(), auths));
    }

    @Test
    public void initWithNullPartitionShouldThrowNPE() {
        final Set<String> auths = new HashSet<>();

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new RFileReaderIterator(null, null, new Configuration(), auths));
    }
}
