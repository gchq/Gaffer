package uk.gov.gchq.gaffer.parquetstore;

import org.apache.commons.lang3.EnumUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StorePropertiesUtil;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;

public class ParquetStorePropertiesUtil extends StorePropertiesUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStorePropertiesUtil.class);

    public static final String DATA_DIR = "parquet.data.dir";
    public static final String TEMP_FILES_DIR = "parquet.temp_data.dir";
    public static final String PARQUET_ROW_GROUP_SIZE_IN_BYTES = "parquet.add_elements.row_group.size";
    public static final String PARQUET_PAGE_SIZE_IN_BYTES = "parquet.add_elements.page.size";
    public static final String PARQUET_AGGREGATE_ON_INGEST = "parquet.add_elements.aggregate";
    public static final String PARQUET_SORT_BY_SPLITS_ON_INGEST = "parquet.add_elements.sort_by_splits";
    public static final String PARQUET_SPLIT_POINTS_SAMPLE_RATE = "parquet.add_elements.split_points.sample_rate";
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

    public static String getDataDir(final StoreProperties parquetStoreProperties) {
        return parquetStoreProperties.getProperty(DATA_DIR);
    }

    public static void setDataDir(final StoreProperties parquetStoreProperties, final String dir) {
        parquetStoreProperties.setProperty(DATA_DIR, dir);
    }

    public static String getTempFilesDir(final StoreProperties parquetStoreProperties) {
        return parquetStoreProperties.getProperty(TEMP_FILES_DIR);
    }

    public static void setTempFilesDir(final StoreProperties parquetStoreProperties, final String dir) {
        parquetStoreProperties.setProperty(TEMP_FILES_DIR, dir);
    }

    public static Integer getThreadsAvailable(final StoreProperties parquetStoreProperties) {
        return Integer.parseInt(parquetStoreProperties.getProperty(PARQUET_THREADS_AVAILABLE, PARQUET_THREADS_AVAILABLE_DEFAULT));
    }

    public static void setThreadsAvailable(final StoreProperties parquetStoreProperties, final Integer threadsAvailable) {
        parquetStoreProperties.setProperty(PARQUET_THREADS_AVAILABLE, threadsAvailable.toString());
    }

    public static Integer getSampleRate(final StoreProperties parquetStoreProperties) {
        return Integer.parseInt(parquetStoreProperties.getProperty(PARQUET_SPLIT_POINTS_SAMPLE_RATE, PARQUET_SPLIT_POINTS_SAMPLE_RATE_DEFAULT));
    }

    public static void setSampleRate(final StoreProperties parquetStoreProperties, final Integer sampleRate) {
        parquetStoreProperties.setProperty(PARQUET_SPLIT_POINTS_SAMPLE_RATE, sampleRate.toString());
    }

    public static Integer getRowGroupSize(final StoreProperties parquetStoreProperties) {
        return Integer.parseInt(parquetStoreProperties.getProperty(PARQUET_ROW_GROUP_SIZE_IN_BYTES, PARQUET_ROW_GROUP_SIZE_IN_BYTES_DEFAULT));
    }

    public static void setRowGroupSize(final StoreProperties parquetStoreProperties, final Integer rowGroupSizeInBytes) {
        parquetStoreProperties.setProperty(PARQUET_ROW_GROUP_SIZE_IN_BYTES, rowGroupSizeInBytes.toString());
    }

    public static Integer getPageSize(final StoreProperties parquetStoreProperties) {
        return Integer.parseInt(parquetStoreProperties.getProperty(PARQUET_PAGE_SIZE_IN_BYTES, PARQUET_PAGE_SIZE_IN_BYTES_DEFAULT));
    }

    public static void setPageSize(final StoreProperties parquetStoreProperties, final int pageSizeInBytes) {
        parquetStoreProperties.setProperty(PARQUET_PAGE_SIZE_IN_BYTES, String.valueOf(pageSizeInBytes));
    }

    public static int getAddElementsOutputFilesPerGroup(final StoreProperties parquetStoreProperties) {
        return Integer.parseInt(parquetStoreProperties.getProperty(PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP, PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP_DEFAULT));
    }

    public static void setAddElementsOutputFilesPerGroup(final StoreProperties parquetStoreProperties, final int outputFilesPerGroup) {
        parquetStoreProperties.setProperty(PARQUET_ADD_ELEMENTS_OUTPUT_FILES_PER_GROUP, String.valueOf(outputFilesPerGroup));
    }

    /**
     * If the Spark master is set in this class then that will be used. Otherwise the Spark default config set on the
     * local machine will be used, if you run your code as a spark-submit command or from the spark-shell.
     * Otherwise a local Spark master will be used.
     *
     * @return The Spark master to be used.
     * @param parquetStoreProperties
     */
    public static String getSparkMaster(final StoreProperties parquetStoreProperties) {
        LOGGER.debug("StoreProperties has Spark master set as: {}", parquetStoreProperties.getProperty(SPARK_MASTER, "Is not set"));
        LOGGER.debug("Spark config has Spark master set as: {}", new SparkConf().get("spark.master", "Is not set"));
        final String sparkMaster = parquetStoreProperties.getProperty(SPARK_MASTER, new SparkConf().get("spark.master", SPARK_MASTER_DEFAULT));
        LOGGER.info("Spark master is set to {}", sparkMaster);
        return sparkMaster;
    }

    public static void setSparkMaster(final StoreProperties parquetStoreProperties, final String sparkMaster) {
        parquetStoreProperties.setProperty(SPARK_MASTER, sparkMaster);
    }

    public static boolean getSkipValidation(final StoreProperties parquetStoreProperties) {
        return Boolean.parseBoolean(parquetStoreProperties.getProperty(PARQUET_SKIP_VALIDATION, PARQUET_SKIP_VALIDATION_DEFAULT));
    }

    public static void setSkipValidation(final StoreProperties parquetStoreProperties, final boolean skipValidation) {
        parquetStoreProperties.setProperty(PARQUET_SKIP_VALIDATION, String.valueOf(skipValidation));
    }

    public static String getJsonSerialiserModules(final StoreProperties parquetStoreProperties) {
        return new StringDeduplicateConcat().apply(
                SketchesJsonModules.class.getName(),
                StorePropertiesUtil.getJsonSerialiserModules(parquetStoreProperties)
        );
    }

    public static boolean getAggregateOnIngest(final StoreProperties parquetStoreProperties) {
        return Boolean.parseBoolean(parquetStoreProperties.getProperty(PARQUET_AGGREGATE_ON_INGEST, PARQUET_AGGREGATE_ON_INGEST_DEFAULT));
    }

    public static void setAggregateOnIngest(final StoreProperties parquetStoreProperties, final boolean aggregateOnIngest) {
        parquetStoreProperties.setProperty(PARQUET_AGGREGATE_ON_INGEST, String.valueOf(aggregateOnIngest));
    }

    public static boolean getSortBySplitsOnIngest(final StoreProperties parquetStoreProperties) {
        return Boolean.parseBoolean(parquetStoreProperties.getProperty(PARQUET_SORT_BY_SPLITS_ON_INGEST, PARQUET_SORT_BY_SPLITS_ON_INGEST_DEFAULT));
    }

    public static void setSortBySplitsOnIngest(final StoreProperties parquetStoreProperties, final boolean sortBySplits) {
        parquetStoreProperties.setProperty(PARQUET_SORT_BY_SPLITS_ON_INGEST, String.valueOf(sortBySplits));
    }

    public static CompressionCodecName getCompressionCodecName(final StoreProperties parquetStoreProperties) throws IllegalArgumentException {
        final String codec = parquetStoreProperties.getProperty(COMPRESSION_CODEC, COMPRESSION_CODEC_DEFAULT);
        if (!EnumUtils.isValidEnum(CompressionCodecName.class, codec)) {
            throw new IllegalArgumentException("Unknown compression codec " + codec);
        }
        return CompressionCodecName.valueOf(codec);
    }

    public static void setCompressionCodecName(final StoreProperties parquetStoreProperties, final String codec) {
        if (!EnumUtils.isValidEnum(CompressionCodecName.class, codec)) {
            throw new IllegalArgumentException("Unknown compression codec " + codec);
        }
        parquetStoreProperties.setProperty(COMPRESSION_CODEC, codec);
    }
}
