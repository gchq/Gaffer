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
package gaffer.accumulostore.operation.hdfs.impl;

import com.fasterxml.jackson.annotation.JsonSetter;
import gaffer.operation.VoidInput;
import gaffer.operation.simple.hdfs.MapReduceOperation;
import gaffer.operation.simple.hdfs.handler.jobfactory.JobInitialiser;
import gaffer.operation.simple.hdfs.handler.mapper.MapperGenerator;
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.List;


/**
 * The <code>SampleDataForSplitPoints</code> operation is for creating a splits file, either for use in a {@link gaffer.accumulostore.operation.hdfs.impl.SplitTable} operation or an
 * {@link gaffer.operation.simple.hdfs.AddElementsFromHdfs} operation.
 * This operation requires an input and output path as well as a path to a file to use as the resulitngSplitsFile.
 * It order to be generic and deal with any type of input file you also need to provide a
 * {@link MapperGenerator} class name and a
 * {@link gaffer.operation.simple.hdfs.handler.jobfactory.JobInitialiser}.
 * <p>
 * For normal operation handlers the operation {@link gaffer.data.elementdefinition.view.View} will be ignored.
 * </p>
 * <b>NOTE</b> - currently this job has to be run as a hadoop job.
 *
 * @see SampleDataForSplitPoints.Builder
 */
public class SampleDataForSplitPoints extends MapReduceOperation<Void, String> implements VoidInput<String> {

    private String resultingSplitsFilePath;
    private boolean validate = true;
    private float proportionToSample;

    /**
     * Used to generate elements from the Hdfs files.
     * For Avro data see {@link gaffer.operation.simple.hdfs.handler.mapper.AvroMapperGenerator}.
     * For Text data see {@link gaffer.operation.simple.hdfs.handler.mapper.TextMapperGenerator}.
     */
    private String mapperGeneratorClassName;

    public SampleDataForSplitPoints() {
        super.setNumReduceTasks(1);
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

    @JsonSetter(value = "mapperGeneratorClassName")
    public void setMapperGeneratorClassName(final String mapperGeneratorClassName) {
        this.mapperGeneratorClassName = mapperGeneratorClassName;
    }

    public void setMapperGeneratorClassName(final Class<? extends MapperGenerator> mapperGeneratorClass) {
        this.mapperGeneratorClassName = mapperGeneratorClass.getName();
    }

    public String getResultingSplitsFilePath() {
        return resultingSplitsFilePath;
    }

    public void setResultingSplitsFilePath(final String resultingSplitsFilePath) {
        this.resultingSplitsFilePath = resultingSplitsFilePath;
    }

    public float getProportionToSample() {
        return proportionToSample;
    }

    public void setProportionToSample(final float proportionToSample) {
        this.proportionToSample = proportionToSample;
    }

    @Override
    public void setNumReduceTasks(final Integer numReduceTasks) {
        throw new IllegalArgumentException(getClass().getSimpleName() + " requires the number of reducers to be 1");
    }

    @Override
    public void setPartitioner(final Class<? extends Partitioner> partitioner) {
        throw new IllegalArgumentException(getClass().getSimpleName() + " is not able to set its own partitioner");
    }

    public static class Builder extends MapReduceOperation.Builder<SampleDataForSplitPoints, Void, String> {
        public Builder() {
            super(new SampleDataForSplitPoints());
        }

        public Builder resultingSplitsFilePath(final String resultingSplitsFilePath) {
            op.setResultingSplitsFilePath(resultingSplitsFilePath);
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

        public Builder proportionToSample(final float proportionToSample) {
            op.setProportionToSample(proportionToSample);
            return this;
        }

        @Override
        public Builder inputPaths(final List<String> inputPaths) {
            return (Builder) super.inputPaths(inputPaths);
        }

        @Override
        public Builder addInputPaths(final List<String> inputPaths) {
            return (Builder) super.addInputPaths(inputPaths);
        }

        @Override
        public Builder addInputPath(final String inputPath) {
            return (Builder) super.addInputPath(inputPath);
        }

        @Override
        public Builder option(final String name, final String value) {
            return (Builder) super.option(name, value);
        }


        @Override
        public Builder outputPath(final String outputPath) {
            return (Builder) super.outputPath(outputPath);
        }

        @Override
        public Builder jobInitialiser(final JobInitialiser jobInitialiser) {
            return (Builder) super.jobInitialiser(jobInitialiser);
        }

        @Override
        public Builder mappers(final Integer numMapTasks) {
            return (Builder) super.mappers(numMapTasks);
        }

    }

}
