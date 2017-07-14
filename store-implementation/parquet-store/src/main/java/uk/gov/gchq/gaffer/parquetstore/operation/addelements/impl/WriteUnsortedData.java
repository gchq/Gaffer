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

package uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.execution.datasources.parquet.ParquetOutputWriter;
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static uk.gov.gchq.gaffer.parquetstore.utils.SparkParquetUtils.configureSparkConfForAddElements;

public class WriteUnsortedData {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteUnsortedData.class);
    private final ParquetStoreProperties props;
    private final Map<String, ParquetOutputWriter> groupToWriter;
    private final Map<String, Integer> groupToFileNumber;
    private final SchemaUtils schemaUtils;
    private final Configuration conf;
    private final Map<String, String[]> groupToGafferProperties;

    public WriteUnsortedData(final ParquetStoreProperties parquetStoreProperties, final SchemaUtils schemaUtils) {
        this.props = parquetStoreProperties;
        this.groupToWriter = new HashMap<>();
        this.groupToFileNumber = new HashMap<>();
        this.schemaUtils = schemaUtils;
        this.conf = new Configuration();
        configureSparkConfForAddElements(conf, parquetStoreProperties);
        this.groupToGafferProperties = calculateGafferPropertes();
    }

    private Map<String, String[]> calculateGafferPropertes() {
        final Set<String> groups = schemaUtils.getGafferSchema().getGroups();
        final Map<String, String[]> groupToProperties = new HashMap<>(groups.size());
        for (final String group : groups) {
            final Set<String> propertiesList = schemaUtils.getGafferSchema().getElement(group).getProperties();
            final String[] propertiesArray = new String[propertiesList.size()];
            propertiesList.toArray(propertiesArray);
            groupToProperties.put(group, propertiesArray);
        }
        return groupToProperties;
    }

    public void writeElements(final Iterator<? extends Element> elements) throws OperationException {
        try {
            // Create a writer for each group
            for (final String group : schemaUtils.getEntityGroups()) {
                groupToWriter.put(group, buildWriter(group, ParquetStoreConstants.VERTEX));
            }
            for (final String group : schemaUtils.getEdgeGroups()) {
                groupToWriter.put(group, buildWriter(group, ParquetStoreConstants.SOURCE));
            }
            // Write elements
            _writeElements(elements);
            // Close the writers
            for (final ParquetOutputWriter writer : groupToWriter.values()) {
                writer.close();
            }
        } catch (final IOException | OperationException e) {
            throw new OperationException("Exception writing elements to " + props.getTempFilesDir(), e);
        }
    }

    private void _writeElements(final Iterator<? extends Element> elements) throws OperationException, IOException {
        while (elements.hasNext()) {
            final Element element = elements.next();
            final String group = element.getGroup();
            ParquetOutputWriter writer = groupToWriter.get(group);
            if (writer != null) {
                writer.writeInternal(schemaUtils.getConverter(group).convertElementToSparkRow(element, groupToGafferProperties.get(group), schemaUtils.getSparkSchema(group)));
            } else {
                LOGGER.warn("Skipped the adding of an Element with Group = {} as that group does not exist in the schema.", group);
            }
        }
    }

    private ParquetOutputWriter buildWriter(final String group, final String column) throws IOException {
        Integer fileNumber = groupToFileNumber.get(group);
        if (fileNumber == null) {
            groupToFileNumber.put(group, 0);
            fileNumber = 0;
        }
        LOGGER.debug("Creating a new writer for group: {}", group + " with file number " + fileNumber);
        final Path filePath = new Path(ParquetStore.getGroupDirectory(group, column,
                props.getTempFilesDir()) + "/part-" + TaskContext.getPartitionId() + "-" + fileNumber + ".gz.parquet");

        conf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA(), ParquetSchemaConverter.checkFieldNames(schemaUtils.getSparkSchema(group)).json());
        final TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, TaskContext.getPartitionId()), 0));
        return new ParquetOutputWriter(filePath.toString(), context);
    }
}
