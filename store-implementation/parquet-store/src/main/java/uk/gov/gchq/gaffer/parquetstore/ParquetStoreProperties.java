/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore;

import org.apache.commons.lang3.EnumUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;

import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;

/**
 * Stores all the user customisable properties required by the {@link ParquetStore}.
 */
public class ParquetStoreProperties extends StoreProperties implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStoreProperties.class);
    public static final String DATA_DIR = "parquet.data.dir";
    public static final String TEMP_FILES_DIR = "parquet.temp_data.dir";
    public static final String PARQUET_ROW_GROUP_SIZE_IN_BYTES = "parquet.add_elements.row_group.size";
    public static final String PARQUET_PAGE_SIZE_IN_BYTES = "parquet.add_elements.page.size";
    public static final String PARQUET_THREADS_AVAILABLE = "parquet.threadsAvailable";
    public static final String PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP = "parquet.add_elements.output_files_per_group";
    public static final String SPARK_MASTER = "spark.master";
    public static final String PARQUET_SKIP_VALIDATION = "parquet.skip_validation";
    public static final String COMPRESSION_CODEC = "parquet.compression.codec";

    // Default values - NB No default values for DATA_DIR or TEMP_FILES_DIR to
    // avoid the inadvertent storage of data in unexpected folders.
    private static final String PARQUET_ROW_GROUP_SIZE_IN_BYTES_DEFAULT = "4194304"; //4MB
    private static final String PARQUET_PAGE_SIZE_IN_BYTES_DEFAULT = "1048576"; //1MB
    public static final String PARQUET_AGGREGATE_ON_INGEST_DEFAULT = "true";
    public static final String PARQUET_SORT_BY_SPLITS_ON_INGEST_DEFAULT = "false";
    private static final String PARQUET_SPLIT_POINTS_SAMPLE_RATE_DEFAULT = "10";
    private static final String PARQUET_THREADS_AVAILABLE_DEFAULT = "3";
    private static final String PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP_DEFAULT = "10";
    private static final String SPARK_MASTER_DEFAULT = "local[*]";
    private static final String PARQUET_SKIP_VALIDATION_DEFAULT = "false";
    private static final String COMPRESSION_CODEC_DEFAULT = "GZIP";
    private static final long serialVersionUID = 7695540336792378185L;

    public ParquetStoreProperties() {
        super(ParquetStore.class);
    }

    public ParquetStoreProperties(final Path propFileLocation) {
        super(propFileLocation, ParquetStore.class);
    }

    public static ParquetStoreProperties loadStoreProperties(final String pathStr) {
        return StoreProperties.loadStoreProperties(pathStr, ParquetStoreProperties.class);
    }

    public static ParquetStoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return StoreProperties.loadStoreProperties(storePropertiesStream, ParquetStoreProperties.class);
    }

    public static ParquetStoreProperties loadStoreProperties(final Path storePropertiesPath) {
        return StoreProperties.loadStoreProperties(storePropertiesPath, ParquetStoreProperties.class);
    }

    public String getDataDir() {
        return get(DATA_DIR);
    }

    public void setDataDir(final String dir) {
        set(DATA_DIR, dir);
    }

    public String getTempFilesDir() {
        return get(TEMP_FILES_DIR);
    }

    public void setTempFilesDir(final String dir) {
        set(TEMP_FILES_DIR, dir);
    }

    public Integer getThreadsAvailable() {
        return Integer.parseInt(get(PARQUET_THREADS_AVAILABLE, PARQUET_THREADS_AVAILABLE_DEFAULT));
    }

    public void setThreadsAvailable(final Integer threadsAvailable) {
        set(PARQUET_THREADS_AVAILABLE, threadsAvailable.toString());
    }

    public Integer getRowGroupSize() {
        return Integer.parseInt(get(PARQUET_ROW_GROUP_SIZE_IN_BYTES, PARQUET_ROW_GROUP_SIZE_IN_BYTES_DEFAULT));
    }

    public void setRowGroupSize(final Integer rowGroupSizeInBytes) {
        set(PARQUET_ROW_GROUP_SIZE_IN_BYTES, rowGroupSizeInBytes.toString());
    }

    public Integer getPageSize() {
        return Integer.parseInt(get(PARQUET_PAGE_SIZE_IN_BYTES, PARQUET_PAGE_SIZE_IN_BYTES_DEFAULT));
    }

    public void setPageSize(final int pageSizeInBytes) {
        set(PARQUET_PAGE_SIZE_IN_BYTES, String.valueOf(pageSizeInBytes));
    }

    public int getAddElementsOutputFilesPerGroup() {
        return Integer.parseInt(get(PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP, PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP_DEFAULT));
    }

    public void setAddElementsOutputFilesPerGroup(final int outputFilesPerGroup) {
        set(PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP, String.valueOf(outputFilesPerGroup));
    }

    /**
     * If the Spark master is set in this class then that will be used. Otherwise the Spark default config set on the
     * local machine will be used, if you run your code as a spark-submit command or from the spark-shell.
     * Otherwise a local Spark master will be used.
     *
     * @return The Spark master to be used.
     */
    public String getSparkMaster() {
        LOGGER.debug("ParquetStoreProperties has Spark master set as: {}", get(SPARK_MASTER, "Is not set"));
        LOGGER.debug("Spark config has Spark master set as: {}", new SparkConf().get("spark.master", "Is not set"));
        final String sparkMaster = get(SPARK_MASTER, new SparkConf().get("spark.master", SPARK_MASTER_DEFAULT));
        LOGGER.info("Spark master is set to {}", sparkMaster);
        return sparkMaster;
    }

    public void setSparkMaster(final String sparkMaster) {
        set(SPARK_MASTER, sparkMaster);
    }

    public boolean getSkipValidation() {
        return Boolean.parseBoolean(get(PARQUET_SKIP_VALIDATION, PARQUET_SKIP_VALIDATION_DEFAULT));
    }

    public void setSkipValidation(final boolean skipValidation) {
        set(PARQUET_SKIP_VALIDATION, String.valueOf(skipValidation));
    }

    @Override
    public String getJsonSerialiserModules() {
        return new StringDeduplicateConcat().apply(
                SketchesJsonModules.class.getName(),
                super.getJsonSerialiserModules()
        );
    }

    public CompressionCodecName getCompressionCodecName() throws IllegalArgumentException {
        final String codec = get(COMPRESSION_CODEC, COMPRESSION_CODEC_DEFAULT);
        if (!EnumUtils.isValidEnum(CompressionCodecName.class, codec)) {
            throw new IllegalArgumentException("Unknown compression codec " + codec);
        }
        return CompressionCodecName.valueOf(codec);
    }

    public void setCompressionCodecName(final String codec) {
        if (!EnumUtils.isValidEnum(CompressionCodecName.class, codec)) {
            throw new IllegalArgumentException("Unknown compression codec " + codec);
        }
        set(COMPRESSION_CODEC, codec);
    }
}
