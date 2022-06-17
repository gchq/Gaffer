package uk.gov.gchq.gaffer.accumulostore.utils;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.utils.LegacySupport.InputConfigurator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These tests simply check that the reflection in the legacy support
 * utility class is able to create and invoke Accumulo methods.
 */
public class LegacySupportTest {

    @TempDir
    static File tempDir;

    @Test
    public void shouldReflectForSetScanAuthorizations() {
        // With
        Configuration conf = new Configuration();
        Authorizations authorisations = new Authorizations();

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.setScanAuthorizations(AccumuloInputFormat.class, conf, authorisations); });
    }

    @Test
    public void shouldReflectForSetInputTableName() {
        // With
        Configuration conf = new Configuration();
        String tableName = "test";

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.setInputTableName(AccumuloInputFormat.class, conf, tableName); });
    }

    @Test
    public void shouldReflectForFetchColumns() {
        // With
        Configuration conf = new Configuration();
        Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs = Arrays.asList(new org.apache.accumulo.core.util.Pair<>(new Text("null"), new Text("null")));

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.fetchColumns(AccumuloInputFormat.class, conf, columnFamilyColumnQualifierPairs); });
    }

    @Test
    public void shouldReflectForAddIterator() {
        // With
        Configuration conf = new Configuration();
        String tableName = "test";
        IteratorSetting setting = new IteratorSetting(1, "", "");

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.addIterator(AccumuloInputFormat.class, conf, setting); });
    }

    @Test
    public void shouldReflectForSetConnectorInfo() {
        // With
        Configuration conf = new Configuration();
        String user = "testUser";
        PasswordToken pass = new PasswordToken();

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.setConnectorInfo(AccumuloInputFormat.class, conf, user, pass); });
    }

    @Test
    public void shouldReflectForSetZooKeeperInstance() {
        // With
        Configuration conf = new Configuration();
        ClientConfiguration withZkHosts = ClientConfiguration.create();

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.setZooKeeperInstance(AccumuloInputFormat.class, conf, withZkHosts); });
    }

    @Test
    public void shouldReflectForSetBatchScan() {
        // With
        Configuration conf = new Configuration();

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.setBatchScan(AccumuloInputFormat.class, conf, false); });
    }

    @Test
    public void shouldReflectForSetRanges() {
        // With
        Configuration conf = new Configuration();
        final List<Range> ranges = new ArrayList<>();

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges); });
    }

    @Test
    public void shouldReflectForGetIterators() {
        // With
        Configuration conf = new Configuration();

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.getIterators(AccumuloInputFormat.class, conf); });
    }

    @Test
    public void shouldReflectForGetFetchedColumns() {
        // With
        Configuration conf = new Configuration();

        // Then
        assertDoesNotThrow(() -> { InputConfigurator.getFetchedColumns(AccumuloInputFormat.class, conf); });
    }

    @Test
    public void shouldReflectForBackwardsCompatibleReaderBuilder() throws IOException {
        // With
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        AccumuloConfiguration accumuloConf = new ConfigurationCopy(DefaultConfiguration.getInstance());
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";
        final File file = new File(filename);
        file.createNewFile();
        String thrown = null;

        // When
        try {
            LegacySupport.BackwardsCompatibleReaderBuilder.create(filename, fs, conf, accumuloConf, false);
        } catch (Throwable th) {
            thrown = ExceptionUtils.getStackTrace(th);
        }

        // Then
        // TODO: Use AsserJ to dump stack trace and be clearer etc.
        // Note we only want to check that the classes are instantiated correctly via reflection, this exception confirms the object was created OK
        assertTrue(thrown.contains("java.io.EOFException: Cannot seek to a negative offset"));
    }

    @Test
    public void shouldReflectForBackwardsCompatibleWriterBuilder() throws IOException {
        // With
        final Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(conf);
        final AccumuloConfiguration accumuloConf = new ConfigurationCopy(DefaultConfiguration.getInstance());
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";
        final File file = new File(filename);
        if (file.exists()) {
            file.delete();
        }

        // Then
        assertDoesNotThrow(() -> { LegacySupport.BackwardsCompatibleWriterBuilder.create(filename, fs, conf, accumuloConf); });
        }

    @Test
    public void shouldReflectForBackwardsCompatibleCachableBlockFileReader() throws IOException {
        // With
        final Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(conf);
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";
        final File file = new File(filename);
        file.createNewFile();
        final Path path = new Path(filename);
        String thrown = null;

        // When
        try {
            LegacySupport.BackwardsCompatibleCachableBlockFileReader.create(fs, path, conf);
        } catch (Throwable th) {
            thrown = ExceptionUtils.getStackTrace(th);
        }

        // Then
        // TODO: Use AsserJ to dump stack trace and be clearer etc.
        // Note we only want to check that the classes are instantiated correctly via reflection, this exception confirms the object was created OK
        assertTrue(thrown.contains("java.io.EOFException: Cannot seek to a negative offset"));
    }

    @Test
    public void shouldReflectForBackwardsCompatibleRFileWriter() throws IOException {
        // With
        final Configuration conf = new Configuration();
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";

        // Then
        assertDoesNotThrow(() -> { LegacySupport.BackwardsCompatibleRFileWriter.create(filename, conf, 1000); });
    }
}
