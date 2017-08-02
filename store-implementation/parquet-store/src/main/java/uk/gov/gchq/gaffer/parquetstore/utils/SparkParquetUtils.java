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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;

/**
 * This is where all the Spark configure required to write out the data is set.
 */
public final class SparkParquetUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkParquetUtils.class);

    private SparkParquetUtils() {
    }

    public static void configureSparkForAddElements(final SparkSession spark, final ParquetStoreProperties props) {
        final Integer numberOfOutputFiles = props.getAddElementsOutputFilesPerGroup();
        String shufflePartitions = spark.conf().getOption("spark.sql.shuffle.partitions").get();
        if (shufflePartitions == null) {
            shufflePartitions = SQLConf.SHUFFLE_PARTITIONS().defaultValueString();
        }
        if (numberOfOutputFiles > Integer.parseInt(shufflePartitions)) {
            LOGGER.debug("Setting the number of Spark shuffle partitions to {}", numberOfOutputFiles);
            spark.conf().set("spark.sql.shuffle.partitions", numberOfOutputFiles);
        }
        final Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        configureSparkConfForAddElements(hadoopConf, props);
    }

    private static void configureSparkConfForAddElements(final Configuration hadoopConf, final ParquetStoreProperties props) {
        LOGGER.debug("Setting the parquet file properties");
        LOGGER.debug("Row group size: {}", props.getRowGroupSize());
        LOGGER.debug("Page size: {}", props.getPageSize());
        hadoopConf.setInt("parquet.block.size", props.getRowGroupSize());
        hadoopConf.setInt("parquet.page.size", props.getPageSize());
        hadoopConf.setInt("parquet.dictionary.page.size", props.getPageSize());
        hadoopConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        hadoopConf.set("parquet.enable.summary-metadata", "false");
    }
}
