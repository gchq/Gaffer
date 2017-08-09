/*
 * Copyright 2017. Crown Copyright
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

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
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


    // Default values
    private static final String DATA_DIR_DEFAULT = "parquet_data";
    private static final String TEMP_FILES_DIR_DEFAULT = ".gaffer/temp_parquet_data";
    private static final String PARQUET_ROW_GROUP_SIZE_IN_BYTES_DEFAULT = "4194304"; //4MB
    private static final String PARQUET_PAGE_SIZE_IN_BYTES_DEFAULT = "1048576"; //1MB
    private static final String PARQUET_THREADS_AVAILABLE_DEFAULT = "3";
    private static final String PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP_DEFAULT = "100";
    private static final String SPARK_MASTER_DEFAULT = "local[*]";
    private static final long serialVersionUID = 7695540336792378185L;

    public ParquetStoreProperties() {
        super();
        this.setStoreClass(ParquetStore.class);
        this.setStorePropertiesClass(getClass());
    }

    public ParquetStoreProperties(final Path propFileLocation) {
        super(propFileLocation);
    }

    public String getDataDir() {
        return get(DATA_DIR, DATA_DIR_DEFAULT);
    }

    public void setDataDir(final String dir) {
        set(DATA_DIR, dir);
    }

    public String getTempFilesDir() {
        return get(TEMP_FILES_DIR, TEMP_FILES_DIR_DEFAULT);
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
}
