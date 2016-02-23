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
package gaffer.operation.simple.hdfs;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import gaffer.operation.AbstractOperation;
import gaffer.operation.VoidInput;
import gaffer.operation.VoidOutput;
import gaffer.operation.simple.hdfs.handler.jobfactory.JobInitialiser;
import gaffer.operation.simple.hdfs.handler.mapper.MapperGenerator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * An <code>AddElementsFromHdfs</code> operation is for adding {@link gaffer.data.element.Element}s from HDFS.
 * This operation requires an input, output and failure path.
 * It order to be generic and deal with any type of input file you also need to provide a
 * {@link gaffer.operation.simple.hdfs.handler.mapper.MapperGenerator} class name and a
 * {@link gaffer.operation.simple.hdfs.handler.jobfactory.JobInitialiser}.
 * <p>
 * For normal operation handlers the operation {@link gaffer.data.elementdefinition.view.View} will be ignored.
 * </p>
 * <b>NOTE</b> - currently this job has to be run as a hadoop job.
 *
 * @see gaffer.operation.simple.hdfs.AddElementsFromHdfs.Builder
 */
public class AddElementsFromHdfs extends AbstractOperation<Void, Void> implements VoidInput<Void>, VoidOutput<Void> {
    private Path inputPath;
    private Path outputPath;
    private Path failurePath;
    private Integer numReduceTasks = null;
    private Integer numMapTasks = null;
    private boolean validate = true;

    /**
     * Used to generate elements from the Hdfs files.
     * For Avro data see {@link gaffer.operation.simple.hdfs.handler.mapper.AvroMapperGenerator}.
     * For Text data see {@link gaffer.operation.simple.hdfs.handler.mapper.TextMapperGenerator}.
     */
    private String mapperGeneratorClassName;

    /**
     * A job initialiser that allows additional job initialisation to be carried out in addition to that done by the
     * store.
     * Most stores will probably require the Job Input to be configured in this initialiser as this is specific to the
     * type of data store in Hdfs.
     * For Avro data see {@link gaffer.operation.simple.hdfs.handler.jobfactory.AvroJobInitialiser}.
     * For Text data see {@link gaffer.operation.simple.hdfs.handler.jobfactory.TextJobInitialiser}.
     */
    private JobInitialiser jobInitialiser;
    private Class<? extends Partitioner> partitioner;

    public Path getInputPath() {
        return inputPath;
    }

    public void setInputPath(final Path inputPath) {
        this.inputPath = inputPath;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(final Path outputPath) {
        this.outputPath = outputPath;
    }

    public Path getFailurePath() {
        return failurePath;
    }

    public void setFailurePath(final Path failurePath) {
        this.failurePath = failurePath;
    }

    public boolean isValidate() {
        return validate;
    }

    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    public String getMapperGeneratorClassName() {
        return mapperGeneratorClassName;
    }

    public void setMapperGeneratorClassName(final String mapperGeneratorClassName) {
        this.mapperGeneratorClassName = mapperGeneratorClassName;
    }

    public void setMapperGeneratorClassName(final Class<? extends MapperGenerator> mapperGeneratorClass) {
        this.mapperGeneratorClassName = mapperGeneratorClass.getName();
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public JobInitialiser getJobInitialiser() {
        return jobInitialiser;
    }

    public void setJobInitialiser(final JobInitialiser jobInitialiser) {
        this.jobInitialiser = jobInitialiser;
    }

    public Integer getNumMapTasks() {
        return numMapTasks;
    }

    public void setNumMapTasks(final Integer numMapTasks) {
        this.numMapTasks = numMapTasks;
    }

    public Integer getNumReduceTasks() {
        return numReduceTasks;
    }

    public void setNumReduceTasks(final Integer numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
    }

    public Class<? extends Partitioner> getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(final Class<? extends Partitioner> partitioner) {
        this.partitioner = partitioner;
    }

    public static class Builder extends AbstractOperation.Builder<AddElementsFromHdfs, Void, Void> {
        public Builder() {
            super(new AddElementsFromHdfs());
        }

        public Builder inputPath(final Path inputPath) {
            op.setInputPath(inputPath);
            return this;
        }

        public Builder outputPath(final Path outputPath) {
            op.setOutputPath(outputPath);
            return this;
        }

        public Builder failurePath(final Path failurePath) {
            op.setFailurePath(failurePath);
            return this;
        }

        public Builder validate(final boolean validate) {
            op.setValidate(validate);
            return this;
        }

        public Builder mapperGenerator(final Class<? extends MapperGenerator> mapperGeneratorClass) {
            op.setMapperGeneratorClassName(mapperGeneratorClass);
            return this;
        }

        public Builder jobInitialiser(final JobInitialiser jobInitialiser) {
            op.setJobInitialiser(jobInitialiser);
            return this;
        }

        public Builder reducers(final Integer numReduceTasks) {
            op.setNumReduceTasks(numReduceTasks);
            return this;
        }

        public Builder mappers(final Integer numMapTasks) {
            op.setNumMapTasks(numMapTasks);
            return this;
        }

        public Builder partioner(final Class<? extends Partitioner> partitioner) {
            op.setPartitioner(partitioner);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
