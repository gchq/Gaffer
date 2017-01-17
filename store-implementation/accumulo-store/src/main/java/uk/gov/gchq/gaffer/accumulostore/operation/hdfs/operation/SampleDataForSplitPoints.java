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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.mapreduce.Partitioner;
import uk.gov.gchq.gaffer.hdfs.operation.MapReduceOperation;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.MapperGenerator;
import uk.gov.gchq.gaffer.operation.VoidInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;


/**
 * The <code>SampleDataForSplitPoints</code> operation is for creating a splits file, either for use in a {@link SplitTable} operation or an
 * {@link uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs} operation.
 * This operation requires an input and output path as well as a path to a file to use as the resulitngSplitsFile.
 * It order to be generic and deal with any type of input file you also need to provide a
 * {@link MapperGenerator} class name and a
 * {@link uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.JobInitialiser}.
 * <p>
 * For normal operation handlers the operation {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View} will be ignored.
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
     * For Avro data see {@link uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.AvroMapperGenerator}.
     * For Text data see {@link uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator}.
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
        if (1 != numReduceTasks) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " requires the number of reducers to be 1");
        }
    }

    @Override
    public void setPartitioner(final Class<? extends Partitioner> partitioner) {
        throw new IllegalArgumentException(getClass().getSimpleName() + " is not able to set its own partitioner");
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.String();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends MapReduceOperation.BaseBuilder<SampleDataForSplitPoints, Void, String, CHILD_CLASS> {
        public BaseBuilder() {
            super(new SampleDataForSplitPoints());
        }

        public CHILD_CLASS resultingSplitsFilePath(final String resultingSplitsFilePath) {
            op.setResultingSplitsFilePath(resultingSplitsFilePath);
            return self();
        }

        public CHILD_CLASS validate(final boolean validate) {
            op.setValidate(validate);
            return self();
        }

        public CHILD_CLASS mapperGenerator(final Class<? extends MapperGenerator> mapperGeneratorClass) {
            op.setMapperGeneratorClassName(mapperGeneratorClass);
            return self();
        }

        public CHILD_CLASS proportionToSample(final float proportionToSample) {
            op.setProportionToSample(proportionToSample);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
