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
package uk.gov.gchq.gaffer.operation.simple.hdfs.operation;

import com.fasterxml.jackson.annotation.JsonSetter;
import uk.gov.gchq.gaffer.operation.VoidInput;
import uk.gov.gchq.gaffer.operation.VoidOutput;
import uk.gov.gchq.gaffer.operation.simple.hdfs.mapper.generator.MapperGenerator;

/**
 * An <code>AddElementsFromHdfs</code> operation is for adding {@link gaffer.data.element.Element}s from HDFS.
 * This operation requires an input, output and failure path.
 * It order to be generic and deal with any type of input file you also need to provide a
 * {@link gaffer.operation.simple.hdfs.mapper.generator.MapperGenerator} class name and a
 * {@link gaffer.operation.simple.hdfs.handler.job.initialiser.JobInitialiser}.
 * <p>
 * For normal operation handlers the operation {@link gaffer.data.elementdefinition.view.View} will be ignored.
 * </p>
 * <b>NOTE</b> - currently this job has to be run as a hadoop job.
 *
 * @see AddElementsFromHdfs.Builder
 */
public class AddElementsFromHdfs extends MapReduceOperation<Void, Void> implements VoidInput<Void>, VoidOutput<Void> {
    private String failurePath;
    private boolean validate = true;

    /**
     * Used to generate elements from the Hdfs files.
     * For Avro data see {@link gaffer.operation.simple.hdfs.mapper.generator.AvroMapperGenerator}.
     * For Text data see {@link gaffer.operation.simple.hdfs.mapper.generator.TextMapperGenerator}.
     */
    private String mapperGeneratorClassName;

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

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends MapReduceOperation.BaseBuilder<AddElementsFromHdfs, Void, Void, CHILD_CLASS> {
        public BaseBuilder() {
            super(new AddElementsFromHdfs());
        }

        public CHILD_CLASS validate(final boolean validate) {
            op.setValidate(validate);
            return self();
        }

        public CHILD_CLASS mapperGenerator(final Class<? extends MapperGenerator> mapperGeneratorClass) {
            op.setMapperGeneratorClassName(mapperGeneratorClass);
            return self();
        }

        public CHILD_CLASS failurePath(final String failurePath) {
            op.setFailurePath(failurePath);
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
