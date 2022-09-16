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
import org.apache.curator.shaded.com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
class LegacySupportTest {

    File tempDir;
    Configuration conf;
    
    @BeforeEach
    public void setUp(){
        conf = new Configuration();
        tempDir = Files.createTempDir();
    }

    @AfterEach
    public void cleanUp(){
        tempDir.deleteOnExit();
    }

    @Test
    void shouldReflectForSetScanAuthorizations() {
        Authorizations authorisations = new Authorizations();

        assertThatNoException().isThrownBy(() -> { InputConfigurator.setScanAuthorizations(AccumuloInputFormat.class, conf, authorisations); });
    }

    @Test
    void shouldReflectForSetInputTableName() {
        String tableName = "test";

        assertThatNoException().isThrownBy(() -> { InputConfigurator.setInputTableName(AccumuloInputFormat.class, conf, tableName); });
    }

    @Test
    void shouldReflectForFetchColumns() {
        Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs = Arrays.asList(new org.apache.accumulo.core.util.Pair<>(new Text("null"), new Text("null")));

        assertThatNoException().isThrownBy(() -> { InputConfigurator.fetchColumns(AccumuloInputFormat.class, conf, columnFamilyColumnQualifierPairs); });
    }

    @Test
    void shouldReflectForAddIterator() {
        IteratorSetting setting = new IteratorSetting(1, "", "");

        assertThatNoException().isThrownBy(() -> { InputConfigurator.addIterator(AccumuloInputFormat.class, conf, setting); });
    }

    @Test
    void shouldReflectForSetConnectorInfo() {
        String user = "testUser";
        PasswordToken pass = new PasswordToken();

        assertThatNoException().isThrownBy(() -> { InputConfigurator.setConnectorInfo(AccumuloInputFormat.class, conf, user, pass); });
    }

    @Test
    void shouldReflectForSetZooKeeperInstance() {
        ClientConfiguration withZkHosts = ClientConfiguration.create();

        assertThatNoException().isThrownBy(() -> { InputConfigurator.setZooKeeperInstance(AccumuloInputFormat.class, conf, withZkHosts); });
    }

    @Test
    void shouldReflectForSetBatchScan() {
        assertThatNoException().isThrownBy(() -> { InputConfigurator.setBatchScan(AccumuloInputFormat.class, conf, false); });
    }

    @Test
    void shouldReflectForSetRanges() {
        final List<Range> ranges = new ArrayList<>();

        assertThatNoException().isThrownBy(() -> { InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges); });
    }

    @Test
    void shouldReflectForGetIterators() {
        assertThatNoException().isThrownBy(() -> { InputConfigurator.getIterators(AccumuloInputFormat.class, conf); });
    }

    @Test
    void shouldReflectForGetFetchedColumns() {
        assertThatNoException().isThrownBy(() -> { InputConfigurator.getFetchedColumns(AccumuloInputFormat.class, conf); });
    }

    @Test
    void shouldReflectForBackwardsCompatibleReaderBuilder() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        AccumuloConfiguration accumuloConf = new ConfigurationCopy(DefaultConfiguration.getInstance());
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";
        final File file = new File(filename);
        file.createNewFile();

        Throwable thrown = catchThrowable(() -> {
            LegacySupport.BackwardsCompatibleReaderBuilder.create(filename, fs, conf, accumuloConf, false);
        });

        // Note we only want to check that the classes are instantiated correctly via reflection, this exception confirms the object was created OK
        assertThat(thrown)
                .isExactlyInstanceOf(RuntimeException.class)
                .hasCauseExactlyInstanceOf(java.lang.reflect.InvocationTargetException.class)
                .hasRootCauseExactlyInstanceOf(java.io.EOFException.class)
                .hasRootCauseMessage("Cannot seek to a negative offset");
    }

    @Test
    void shouldReflectForBackwardsCompatibleWriterBuilder() throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        final AccumuloConfiguration accumuloConf = new ConfigurationCopy(DefaultConfiguration.getInstance());
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";

        assertThatNoException().isThrownBy(() -> { LegacySupport.BackwardsCompatibleWriterBuilder
            .create(filename, fs, conf, accumuloConf); });
        }

    @Test
    void shouldReflectForBackwardsCompatibleCachableBlockFileReader() throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";
        final File file = new File(filename);
        file.createNewFile();
        final Path path = new Path(filename);

        Throwable thrown = catchThrowable(() -> {
            LegacySupport.BackwardsCompatibleCachableBlockFileReader.create(fs, path, conf);
        });

        // Note we only want to check that the classes are instantiated correctly via reflection, this exception confirms the object was created OK
        assertThat(thrown)
                .isExactlyInstanceOf(RuntimeException.class)
                .hasCauseExactlyInstanceOf(java.lang.reflect.InvocationTargetException.class)
                .hasRootCauseExactlyInstanceOf(java.io.EOFException.class)
                .hasRootCauseMessage("Cannot seek to a negative offset");
    }

    @Test
    void shouldReflectForBackwardsCompatibleRFileWriter() throws IOException {
        final String filenameTemp = tempDir.getAbsolutePath();
        final String filename = filenameTemp + "/file.rf";

        assertThatNoException().isThrownBy(() -> { LegacySupport.BackwardsCompatibleRFileWriter.create(filename, conf, 1000); });
    }
}
