/*
 * Copyright 2022 Crown Copyright
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * These tests simply check that the reflection in the legacy support
 * utility class is able to create and invoke Accumulo methods.
 */
public class LegacySupportTest {

    @TempDir
    static File tempDir;

    @Test
    public void shouldReflectForSetScanAuthorizations() {
        // Given
        Configuration conf = new Configuration();
        Authorizations authorisations = new Authorizations();

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.setScanAuthorizations(AccumuloInputFormat.class, conf, authorisations); });
    }

    @Test
    public void shouldReflectForSetInputTableName() {
        // Given
        Configuration conf = new Configuration();
        String tableName = "test";

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.setInputTableName(AccumuloInputFormat.class, conf, tableName); });
    }

    @Test
    public void shouldReflectForFetchColumns() {
        // Given
        Configuration conf = new Configuration();
        Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs = Arrays.asList(new org.apache.accumulo.core.util.Pair<>(new Text("null"), new Text("null")));

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.fetchColumns(AccumuloInputFormat.class, conf, columnFamilyColumnQualifierPairs); });
    }

    @Test
    public void shouldReflectForAddIterator() {
        // Given
        Configuration conf = new Configuration();
        String tableName = "test";
        IteratorSetting setting = new IteratorSetting(1, "", "");

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.addIterator(AccumuloInputFormat.class, conf, setting); });
    }

    @Test
    public void shouldReflectForSetConnectorInfo() {
        // Given
        Configuration conf = new Configuration();
        String user = "testUser";
        PasswordToken pass = new PasswordToken();

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.setConnectorInfo(AccumuloInputFormat.class, conf, user, pass); });
    }

    @Test
    public void shouldReflectForSetZooKeeperInstance() {
        // Given
        Configuration conf = new Configuration();
        ClientConfiguration withZkHosts = ClientConfiguration.create();

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.setZooKeeperInstance(AccumuloInputFormat.class, conf, withZkHosts); });
    }

    @Test
    public void shouldReflectForSetBatchScan() {
        // Given
        Configuration conf = new Configuration();

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.setBatchScan(AccumuloInputFormat.class, conf, false); });
    }

    @Test
    public void shouldReflectForSetRanges() {
        // Given
        Configuration conf = new Configuration();
        final List<Range> ranges = new ArrayList<>();

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges); });
    }

    @Test
    public void shouldReflectForGetIterators() {
        // Given
        Configuration conf = new Configuration();

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.getIterators(AccumuloInputFormat.class, conf); });
    }

    @Test
    public void shouldReflectForGetFetchedColumns() {
        // Given
        Configuration conf = new Configuration();

        // Then
        assertThatNoException().isThrownBy(() -> { InputConfigurator.getFetchedColumns(AccumuloInputFormat.class, conf); });
    }

    @Test
    public void shouldReflectForBackwardsCompatibleReaderBuilder() throws IOException {
        // Given
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        AccumuloConfiguration accumuloConf = new ConfigurationCopy(DefaultConfiguration.getInstance());
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";
        final File file = new File(filename);
        file.createNewFile();

        // When
        Throwable thrown = catchThrowable(() -> {
            LegacySupport.BackwardsCompatibleReaderBuilder.create(filename, fs, conf, accumuloConf, false);
        });

        // Then
        // Note we only want to check that the classes are instantiated correctly via reflection, this exception confirms the object was created OK
        assertThat(thrown)
                .isExactlyInstanceOf(RuntimeException.class)
                .hasCauseExactlyInstanceOf(java.lang.reflect.InvocationTargetException.class)
                .hasRootCauseExactlyInstanceOf(java.io.EOFException.class)
                .hasRootCauseMessage("Cannot seek to a negative offset");
    }

    @Test
    public void shouldReflectForBackwardsCompatibleWriterBuilder() throws IOException {
        // Given
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
        assertThatNoException().isThrownBy(() -> { LegacySupport.BackwardsCompatibleWriterBuilder.create(filename, fs, conf, accumuloConf); });
        }

    @Test
    public void shouldReflectForBackwardsCompatibleCachableBlockFileReader() throws IOException {
        // Given
        final Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(conf);
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";
        final File file = new File(filename);
        file.createNewFile();
        final Path path = new Path(filename);

        // When
        Throwable thrown = catchThrowable(() -> {
            LegacySupport.BackwardsCompatibleCachableBlockFileReader.create(fs, path, conf);
        });

        // Then
        // Note we only want to check that the classes are instantiated correctly via reflection, this exception confirms the object was created OK
        assertThat(thrown)
                .isExactlyInstanceOf(RuntimeException.class)
                .hasCauseExactlyInstanceOf(java.lang.reflect.InvocationTargetException.class)
                .hasRootCauseExactlyInstanceOf(java.io.EOFException.class)
                .hasRootCauseMessage("Cannot seek to a negative offset");
    }

    @Test
    public void shouldReflectForBackwardsCompatibleRFileWriter() throws IOException {
        // Given
        final Configuration conf = new Configuration();
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";

        // Then
        assertThatNoException().isThrownBy(() -> { LegacySupport.BackwardsCompatibleRFileWriter.create(filename, conf, 1000); });
    }
}
