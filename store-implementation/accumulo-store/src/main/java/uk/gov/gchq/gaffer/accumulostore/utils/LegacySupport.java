/*
 * Copyright 2022-2023 Crown Copyright
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ratelimit.NullRateLimiter;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A utility class to provide static methods which map to a single
 * import. This is to mitigate Accumulo code and package name changes
 * between versions 1.x and 2.x of Accumulo. These changes would
 * otherwise require duplication of any modules which imported
 * Accumulo packages which had changed names or required code changes
 * to remain compatible with both versions of Accumulo.
 */
public class LegacySupport {
    private static boolean usingAccumulo2 = true;
    private static final String INIT_ERROR = "Failed initialing Accumulo class. Ensure Accumulo version is supported";
    private static Class<?> inputConfiguratorClazz;
    private static Class<?> noCryptoServiceClazz;
    private static Class<?> cryptoServiceClazz;
    private static Class<?> cachableBuilderClazz;
    private static Class<?> fileOperationsReaderBuilderClazz;
    private static Class<?> fileOperationsWriterBuilderClazz;
    private static Class<?> fsdataoutputstreamclazz;
    private static Class<?> siteConfigurationClazz;
    private static Class<?> needsFileClazz;
    private static Class<?> needsFileOrOuputStreamClazz;
    private static Class<?> needsTableConfigurationClazz;
    private static Class<?> openWriterOperationBuilderClazz;
    private static Class<?> openReaderOperationBuilderClazz;
    private static Class<?> cachableBlockFileWriterClazz;
    private static Class<?> blockFileWriterClazz;
    private static Class<?> blockFileReaderClazz;
    private static Class<?> blockCacheClazz;

    static {
        try {
            inputConfiguratorClazz = Class.forName("org.apache.accumulo.core.clientImpl.mapreduce.lib.InputConfigurator");
            noCryptoServiceClazz = Class.forName("org.apache.accumulo.core.cryptoImpl.NoCryptoService");
            cryptoServiceClazz = Class.forName("org.apache.accumulo.core.spi.crypto.CryptoService");
            cachableBuilderClazz = Class.forName("org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile$CachableBuilder");
            fileOperationsReaderBuilderClazz = Class.forName("org.apache.accumulo.core.file.FileOperations$ReaderBuilder");
            fileOperationsWriterBuilderClazz = Class.forName("org.apache.accumulo.core.file.FileOperations$WriterBuilder");
            fsdataoutputstreamclazz = Class.forName("org.apache.hadoop.fs.FSDataOutputStream");
        } catch (final ClassNotFoundException e) {
            // Fall back to using Accumulo 1 classes
            usingAccumulo2 = false;
        }
        if (!usingAccumulo2) {
            try {
                inputConfiguratorClazz = Class.forName("org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator");
                siteConfigurationClazz = Class.forName("org.apache.accumulo.core.conf.SiteConfiguration");
                needsFileClazz = Class.forName("org.apache.accumulo.core.file.FileOperations$NeedsFile");
                needsFileOrOuputStreamClazz = Class.forName("org.apache.accumulo.core.file.FileOperations$NeedsFileOrOuputStream");
                needsTableConfigurationClazz = Class.forName("org.apache.accumulo.core.file.FileOperations$NeedsTableConfiguration");
                openWriterOperationBuilderClazz = Class.forName("org.apache.accumulo.core.file.FileOperations$OpenWriterOperationBuilder");
                openReaderOperationBuilderClazz = Class.forName("org.apache.accumulo.core.file.FileOperations$OpenReaderOperationBuilder");
                cachableBlockFileWriterClazz = Class.forName("org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile$Writer");
                blockFileWriterClazz = Class.forName("org.apache.accumulo.core.file.blockfile.BlockFileWriter");
                blockFileReaderClazz = Class.forName("org.apache.accumulo.core.file.blockfile.BlockFileReader");
                blockCacheClazz = Class.forName("org.apache.accumulo.core.file.blockfile.cache.BlockCache");
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException("Failed initialing Accumulo support classes. Ensure Accumulo version is supported", e);
            }
        }
    }
    public static class InputConfigurator {
        public static void setScanAuthorizations(final Class<?> implementingClass, final Configuration conf, final Authorizations auths) {
            try {
                Method setScanAuthorizations = inputConfiguratorClazz.getMethod("setScanAuthorizations", Class.class, Configuration.class, Authorizations.class);
                setScanAuthorizations.invoke(null, implementingClass, conf, auths);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static void setInputTableName(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf, final String tableName) {
            try {
                Method setInputTableName = inputConfiguratorClazz.getMethod("setInputTableName", Class.class, Configuration.class, String.class);
                setInputTableName.invoke(null, accumuloInputFormatClass, conf, tableName);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static void fetchColumns(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf,
                                        final Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs) {
            try {
                Method fetchColumns = inputConfiguratorClazz.getMethod("fetchColumns", Class.class, Configuration.class, Collection.class);
                fetchColumns.invoke(null, accumuloInputFormatClass, conf, columnFamilyColumnQualifierPairs);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static void addIterator(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf, final IteratorSetting elementPreFilter) {
            try {
                Method addIterator = inputConfiguratorClazz.getMethod("addIterator", Class.class, Configuration.class, IteratorSetting.class);
                addIterator.invoke(null, accumuloInputFormatClass, conf, elementPreFilter);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static void setConnectorInfo(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf, final String user, final AuthenticationToken token) {
            try {
                Method setConnectorInfo = inputConfiguratorClazz.getMethod("setConnectorInfo", Class.class, Configuration.class, String.class, AuthenticationToken.class);
                setConnectorInfo.invoke(null, accumuloInputFormatClass, conf, user, token);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static void setZooKeeperInstance(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf, final ClientConfiguration withZkHosts) {
            try {
                Method setZooKeeperInstance = inputConfiguratorClazz.getMethod("setZooKeeperInstance", Class.class, Configuration.class, ClientConfiguration.class);
                setZooKeeperInstance.invoke(null, accumuloInputFormatClass, conf, withZkHosts);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static void setBatchScan(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf, final boolean enableFeature) {
            try {
                Method setBatchScan = inputConfiguratorClazz.getMethod("setBatchScan", Class.class, Configuration.class, boolean.class);
                setBatchScan.invoke(null, accumuloInputFormatClass, conf, enableFeature);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static void setRanges(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf, final Collection<Range> ranges) {
            try {
                Method setRanges = inputConfiguratorClazz.getMethod("setRanges", Class.class, Configuration.class, Collection.class);
                setRanges.invoke(null, accumuloInputFormatClass, conf, ranges);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static List<IteratorSetting> getIterators(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf) {
            try {
                Method getIterators = inputConfiguratorClazz.getMethod("getIterators", Class.class, Configuration.class);
                return (List<IteratorSetting>) getIterators.invoke(null, accumuloInputFormatClass, conf);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }

        public static Set<Pair<Text, Text>> getFetchedColumns(final Class<AccumuloInputFormat> accumuloInputFormatClass, final Configuration conf) {
            try {
                Method getFetchedColumns = inputConfiguratorClazz.getMethod("getFetchedColumns", Class.class, Configuration.class);
                return (Set<Pair<Text, Text>>) getFetchedColumns.invoke(null, accumuloInputFormatClass, conf);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "Specific Exception types vary depending on library version")
    public static class BackwardsCompatibleReaderBuilder {
        public static FileSKVIterator create(final String filename, final FileSystem fs, final Configuration fsConf,
                                             final AccumuloConfiguration tableConfiguration, final boolean seekBeginning) {
            final Method forFile, withTableConfiguration, seekToBeginning, builderBuild;
            Object builder = FileOperations.getInstance();
            try {
                builder = FileOperations.class.getMethod("newReaderBuilder").invoke(builder);
                if (usingAccumulo2) {
                    forFile = fileOperationsReaderBuilderClazz.getMethod("forFile", String.class, FileSystem.class, Configuration.class, cryptoServiceClazz);
                    withTableConfiguration = fileOperationsReaderBuilderClazz.getMethod("withTableConfiguration", AccumuloConfiguration.class);
                    seekToBeginning = fileOperationsReaderBuilderClazz.getMethod("seekToBeginning", boolean.class);
                    builderBuild = fileOperationsReaderBuilderClazz.getMethod("build");
                    builder = forFile.invoke(builder, filename, fs, fsConf, noCryptoServiceClazz.getConstructor().newInstance());
                } else {
                    forFile = needsFileClazz.getMethod("forFile", String.class, FileSystem.class, Configuration.class);
                    withTableConfiguration = needsTableConfigurationClazz.getMethod("withTableConfiguration", AccumuloConfiguration.class);
                    seekToBeginning = openReaderOperationBuilderClazz.getMethod("seekToBeginning", boolean.class);
                    builderBuild = openReaderOperationBuilderClazz.getMethod("build");
                    builder = forFile.invoke(builder, filename, fs, fsConf);
                }
                builder = seekToBeginning.invoke(builder, seekBeginning);
                builder = withTableConfiguration.invoke(builder, tableConfiguration);
                return (FileSKVIterator) builderBuild.invoke(builder);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }
    }

    public static class BackwardsCompatibleWriterBuilder {
        public static FileSKVWriter create(final String filename, final FileSystem fs, final Configuration fsConf,
                                           final AccumuloConfiguration tableConfiguration) {
            final Method forFile, withTableConfiguration, builderBuild;
            Object builder = FileOperations.getInstance();
            try {
                builder = FileOperations.class.getMethod("newWriterBuilder").invoke(builder);
                if (usingAccumulo2) {
                    forFile = fileOperationsWriterBuilderClazz.getMethod("forFile", String.class, FileSystem.class, Configuration.class, cryptoServiceClazz);
                    withTableConfiguration = fileOperationsWriterBuilderClazz.getMethod("withTableConfiguration", AccumuloConfiguration.class);
                    builderBuild = fileOperationsWriterBuilderClazz.getMethod("build");
                    builder = forFile.invoke(builder, filename, fs, fsConf, noCryptoServiceClazz.getConstructor().newInstance());
                } else {
                    forFile = needsFileOrOuputStreamClazz.getMethod("forFile", String.class, FileSystem.class, Configuration.class);
                    withTableConfiguration = needsTableConfigurationClazz.getMethod("withTableConfiguration", AccumuloConfiguration.class);
                    builderBuild = openWriterOperationBuilderClazz.getMethod("build");
                    builder = forFile.invoke(builder, filename, fs, fsConf);
                }
                builder = withTableConfiguration.invoke(builder, tableConfiguration);
                return (FileSKVWriter) builderBuild.invoke(builder);
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "Specific Exception types vary depending on library version")
    public static class BackwardsCompatibleCachableBlockFileReader {
        public static RFile.Reader create(final FileSystem fs, final Path path, final Configuration fsConf) {
            final RFile.Reader reader;
            try {
                if (usingAccumulo2) {
                    Object builder = cachableBuilderClazz.getConstructor().newInstance();
                    Method fsPathMethod = cachableBuilderClazz.getMethod("fsPath", FileSystem.class, Path.class);
                    Method confMethod = cachableBuilderClazz.getMethod("conf", Configuration.class);
                    Method cryptoServiceMethod = cachableBuilderClazz.getMethod("cryptoService", cryptoServiceClazz);
                    fsPathMethod.invoke(builder, fs, path);
                    confMethod.invoke(builder, fsConf);
                    cryptoServiceMethod.invoke(builder, noCryptoServiceClazz.getConstructor().newInstance());
                    reader = RFile.Reader.class.getConstructor(cachableBuilderClazz).newInstance(builder);
                } else {
                    AccumuloConfiguration accumuloConfiguration = (AccumuloConfiguration) siteConfigurationClazz.getMethod("getInstance").invoke(null);
                    Constructor cachableBlockFileReaderConstructor = CachableBlockFile.Reader.class.getConstructor(FileSystem.class, Path.class, Configuration.class, blockCacheClazz,
                            blockCacheClazz, AccumuloConfiguration.class);
                    Object cacheableReader = cachableBlockFileReaderConstructor.newInstance(
                            fs,
                            path,
                            fsConf,
                            null,
                            null,
                            accumuloConfiguration
                    );
                    reader = RFile.Reader.class.getConstructor(blockFileReaderClazz).newInstance(cacheableReader);
                }
                return reader;
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "Specific Exception types vary depending on library version")
    public static class BackwardsCompatibleRFileWriter {
        public static RFile.Writer create(final String file, final Configuration fsConf, final int blocksize) {
            final RFile.Writer writer;
            final Constructor bcFileWriterConstructor, cachableBlockFileWriterConstructor, rFileWriterConstructor;
            try {
                if (usingAccumulo2) {
                    bcFileWriterConstructor = BCFile.Writer.class.getConstructor(fsdataoutputstreamclazz, RateLimiter.class, String.class,
                            Configuration.class, cryptoServiceClazz);
                    Object bcFileWriter = bcFileWriterConstructor.newInstance(
                            FileSystem.get(fsConf).create(new Path(file)),
                            NullRateLimiter.INSTANCE,
                            Compression.COMPRESSION_NONE,
                            fsConf,
                            noCryptoServiceClazz.getConstructor().newInstance()
                    );
                    rFileWriterConstructor = RFile.Writer.class.getConstructor(BCFile.Writer.class, int.class);
                    writer = (RFile.Writer) rFileWriterConstructor.newInstance(bcFileWriter, blocksize);
                } else {
                    cachableBlockFileWriterConstructor = cachableBlockFileWriterClazz.getConstructor(FileSystem.class, Path.class, String.class, RateLimiter.class,
                            Configuration.class, AccumuloConfiguration.class);
                    AccumuloConfiguration accumuloConfiguration = (AccumuloConfiguration) AccumuloConfiguration.class.getMethod("getDefaultConfiguration").invoke(null);
                    Object cachableBlockFileWriter = cachableBlockFileWriterConstructor.newInstance(
                            FileSystem.get(fsConf),
                            new Path(file),
                            Compression.COMPRESSION_NONE,
                            null,
                            fsConf,
                            accumuloConfiguration
                    );
                    rFileWriterConstructor = RFile.Writer.class.getConstructor(blockFileWriterClazz, int.class);
                    writer = (RFile.Writer) rFileWriterConstructor.newInstance(cachableBlockFileWriter, blocksize);
                }
                return writer;
            } catch (final Exception e) {
                throw new RuntimeException(INIT_ERROR, e);
            }
        }
    }
}
