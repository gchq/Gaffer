/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.hdfs.operation;

import org.apache.hadoop.mapreduce.Partitioner;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.JobInitialiser;
import uk.gov.gchq.gaffer.operation.Operation;

import java.util.Map;

/**
 * An {@code AddElementsFromHdfs} operation is for adding {@link uk.gov.gchq.gaffer.data.element.Element}s from HDFS.
 * This operation requires an input, output and failure path.
 * For each input file you must also provide a {@link uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.MapperGenerator} class name
 * as part of a pair (input, mapperGeneratorClassName).
 * In order to be generic and deal with any type of input file you also need to provide a
 * {@link uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.JobInitialiser}.
 * You will need to write your own {@link uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.MapperGenerator} to convert the input
 * data into gaffer {@link uk.gov.gchq.gaffer.data.element.Element}s. This can
 * be as simple as delegating to your {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator}
 * class, however it can be more complex and make use of the configuration in
 * {@link org.apache.hadoop.mapreduce.MapContext}.
 * <p>
 * For normal operation handlers the operation {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View} will be ignored.
 * </p>
 * <b>NOTE</b> - currently this job has to be run as a hadoop job.
 *
 * @see MapReduce
 * @see Builder
 */
public class AddElementsFromHdfs implements
        Operation,
        MapReduce {
    @Required
    private String failurePath;

    /**
     * Path to a folder to store temporary files during the add elements operation.
     */
    private String workingPath;

    private boolean validate = true;

    /**
     * Used to generate elements from the Hdfs files.
     * For Avro data see {@link uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.AvroMapperGenerator}.
     * For Text data see {@link uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator}.
     */
    @Required
    private Map<String, String> inputMapperPairs;

    @Required
    private String outputPath;

    @Required
    private JobInitialiser jobInitialiser;

    private Integer numMapTasks;
    private Integer minMapTasks;
    private Integer maxMapTasks;

    private Integer numReduceTasks;
    private Integer minReduceTasks;
    private Integer maxReduceTasks;

    private boolean useProvidedSplits;
    private String splitsFilePath;

    private Class<? extends Partitioner> partitioner;
    private Map<String, String> options;

    public String getFailurePath() {
        return failurePath;
    }

    public void setFailurePath(final String failurePath) {
        this.failurePath = failurePath;
    }

    public boolean isValidate() {
        return validate;
    }

    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    @Override
    public Map<String, String> getInputMapperPairs() {
        return inputMapperPairs;
    }

    @Override
    public void setInputMapperPairs(final Map<String, String> inputMapperPairs) {
        this.inputMapperPairs = inputMapperPairs;
    }

    @Override
    public String getOutputPath() {
        return outputPath;
    }

    @Override
    public void setOutputPath(final String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public JobInitialiser getJobInitialiser() {
        return jobInitialiser;
    }

    @Override
    public void setJobInitialiser(final JobInitialiser jobInitialiser) {
        this.jobInitialiser = jobInitialiser;
    }

    @Override
    public Integer getNumMapTasks() {
        return numMapTasks;
    }

    @Override
    public void setNumMapTasks(final Integer numMapTasks) {
        this.numMapTasks = numMapTasks;
    }

    @Override
    public Integer getNumReduceTasks() {
        return numReduceTasks;
    }

    @Override
    public void setNumReduceTasks(final Integer numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
    }

    @Override
    public Integer getMinMapTasks() {
        return minMapTasks;
    }

    @Override
    public void setMinMapTasks(final Integer minMapTasks) {
        this.minMapTasks = minMapTasks;
    }

    @Override
    public Integer getMaxMapTasks() {
        return maxMapTasks;
    }

    @Override
    public void setMaxMapTasks(final Integer maxMapTasks) {
        this.maxMapTasks = maxMapTasks;
    }

    @Override
    public Integer getMinReduceTasks() {
        return minReduceTasks;
    }

    @Override
    public void setMinReduceTasks(final Integer minReduceTasks) {
        this.minReduceTasks = minReduceTasks;
    }

    @Override
    public Integer getMaxReduceTasks() {
        return maxReduceTasks;
    }

    @Override
    public void setMaxReduceTasks(final Integer maxReduceTasks) {
        this.maxReduceTasks = maxReduceTasks;
    }

    @Override
    public boolean isUseProvidedSplits() {
        return useProvidedSplits;
    }

    @Override
    public void setUseProvidedSplits(final boolean useProvidedSplits) {
        this.useProvidedSplits = useProvidedSplits;
    }

    @Override
    public String getSplitsFilePath() {
        return splitsFilePath;
    }

    @Override
    public void setSplitsFilePath(final String splitsFilePath) {
        this.splitsFilePath = splitsFilePath;
    }

    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return partitioner;
    }

    @Override
    public void setPartitioner(final Class<? extends Partitioner> partitioner) {
        this.partitioner = partitioner;
    }

    public String getWorkingPath() {
        return workingPath;
    }

    public void setWorkingPath(final String workingPath) {
        this.workingPath = workingPath;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public AddElementsFromHdfs shallowClone() {
        return new AddElementsFromHdfs.Builder()
                .failurePath(failurePath)
                .workingPath(workingPath)
                .validate(validate)
                .inputMapperPairs(inputMapperPairs)
                .outputPath(outputPath)
                .jobInitialiser(jobInitialiser)
                .mappers(numMapTasks)
                .reducers(numReduceTasks)
                .minMappers(minMapTasks)
                .minReducers(minReduceTasks)
                .maxMappers(maxMapTasks)
                .maxReducers(maxReduceTasks)
                .useProvidedSplits(useProvidedSplits)
                .splitsFilePath(splitsFilePath)
                .partitioner(partitioner)
                .options(options)
                .build();
    }

    public static final class Builder extends Operation.BaseBuilder<AddElementsFromHdfs, Builder>
            implements MapReduce.Builder<AddElementsFromHdfs, Builder> {
        public Builder() {
            super(new AddElementsFromHdfs());
        }

        public Builder validate(final boolean validate) {
            _getOp().setValidate(validate);
            return _self();
        }

        @Override
        public Builder inputMapperPairs(final Map<String, String> inputMapperPairs) {
            _getOp().setInputMapperPairs(inputMapperPairs);
            return _self();
        }

        public Builder failurePath(final String failurePath) {
            _getOp().setFailurePath(failurePath);
            return _self();
        }

        public Builder workingPath(final String workingPath) {
            _getOp().setWorkingPath(workingPath);
            return _self();
        }
    }
}
