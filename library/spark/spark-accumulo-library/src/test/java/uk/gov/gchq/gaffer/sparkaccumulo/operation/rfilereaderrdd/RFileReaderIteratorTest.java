package uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class RFileReaderIteratorTest {

    @Test
    @DisplayName("Init RFileReaderIterator with invalid AccumuloTablet file throws RunTimeException")
    void init_withAnyConfig() {
        final AccumuloTablet partition = new AccumuloTablet(0, 0, "a", "b");
        final TaskContext taskContext = mock(TaskContext.class);
        final Set<String> auths = new HashSet<>();

        final Configuration configuration = new Configuration();
        configuration.set("any key", "blahh");

        final RFileReaderIterator iterator = new RFileReaderIterator(partition, taskContext, configuration, auths);

        assertFalse(iterator.hasNext());
    }

    @Test
    @DisplayName("Empty Partition/AccumuloTablet does not have a next iterator")
    void init_withEmptyAccumuloTablet() {
        final Partition partition = new AccumuloTablet(0, 0, "a", "b");
        final TaskContext taskContext = mock(TaskContext.class);
        final Set<String> auths = new HashSet<>();

        final RFileReaderIterator iterator = new RFileReaderIterator(partition, taskContext, new Configuration(), auths);

        assertFalse(iterator.hasNext());
    }

    @Test
    @DisplayName("Null auths does not have a next iterator")
    void init_withNullAuths() {
        final Partition partition = new AccumuloTablet(0, 0, "a", "b");
        final TaskContext taskContext = mock(TaskContext.class);

        final RFileReaderIterator iterator = new RFileReaderIterator(partition, taskContext, new Configuration(), null);

        assertFalse(iterator.hasNext());
    }

    @Test
    @DisplayName("Invalid Partition Type (Accumulo Tablet) throws RunTimeException for thrown IOException on init")
    void init_withInvalidFileAddedToAccumulo() {
        final AccumuloTablet partition = new AccumuloTablet(0, 0, "a", "b");
        final TaskContext taskContext = mock(TaskContext.class);
        final Set<String> auths = new HashSet<>();

        partition.addRFile("invalid file");

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            new RFileReaderIterator(partition, taskContext, new Configuration(), auths);
        });

        assertEquals("IOException initialising RFileReaderIterator", exception.getMessage());
    }

    @Test
    @DisplayName("Init RFileReaderIterator with null TaskContext should throw NPE")
    void init_nullTaskContent() {
        final Partition partition = new AccumuloTablet(0, 0, "a", "b");
        final Set<String> auths = new HashSet<>();

        assertThrows(NullPointerException.class, () -> {
            new RFileReaderIterator(partition, null, new Configuration(), auths);
        });
    }

    @Test
    @DisplayName("Init RFileReaderIterator with null Partition should throw NPE")
    void init_nullPartition() {
        final Set<String> auths = new HashSet<>();

        assertThrows(NullPointerException.class, () -> {
            new RFileReaderIterator(null, null, new Configuration(), auths);
        });
    }
}