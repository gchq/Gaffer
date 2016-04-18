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

import gaffer.operation.VoidInput;
import gaffer.operation.VoidOutput;
import gaffer.operation.simple.hdfs.handler.jobfactory.JobInitialiser;
import gaffer.operation.simple.hdfs.handler.mapper.MapperGenerator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.List;

/**
 * An <code>AddElementsFromHdfs</code> operation is for adding {@link gaffer.data.element.Element}s from HDFS.
 * This operation requires an input, output and failure path.
 * It order to be generic and deal with any type of input file you also need to provide a
 * {@link MapperGenerator} class name and a
 * {@link JobInitialiser}.
 * <p>
 * For normal operation handlers the operation {@link gaffer.data.elementdefinition.view.View} will be ignored.
 * </p>
 * <b>NOTE</b> - currently this job has to be run as a hadoop job.
 *
 * @see AddElementsFromHdfs.Builder
 */
public class AddElementsFromHdfs extends MapReduceOperation<Void, Void> implements VoidInput<Void>, VoidOutput<Void> {
    private Path failurePath;
    private boolean validate = true;

    /**
     * Used to generate elements from the Hdfs files.
     * For Avro data see {@link gaffer.operation.simple.hdfs.handler.mapper.AvroMapperGenerator}.
     * For Text data see {@link gaffer.operation.simple.hdfs.handler.mapper.TextMapperGenerator}.
     */
    private String mapperGeneratorClassName;

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

    public static class Builder extends MapReduceOperation.Builder<AddElementsFromHdfs, Void, Void> {
        public Builder() {
            super(new AddElementsFromHdfs());
        }

        public Builder validate(final boolean validate) {
            op.setValidate(validate);
            return this;
        }

        public Builder mapperGenerator(final Class<? extends MapperGenerator> mapperGeneratorClass) {
            op.setMapperGeneratorClassName(mapperGeneratorClass);
            return this;
        }

        public Builder failurePath(final Path failurePath) {
            op.setFailurePath(failurePath);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            return (Builder) super.option(name, value);
        }

        @Override
        protected Builder inputPaths(final List<Path> inputPaths) {
            return (Builder) super.inputPaths(inputPaths);
        }

        @Override
        protected Builder addInputPaths(final List<Path> inputPaths) {
            return (Builder) super.addInputPaths(inputPaths);
        }

        @Override
        protected Builder addInputPath(final Path inputPath) {
            return (Builder) super.addInputPath(inputPath);
        }

        @Override
        public Builder outputPath(final Path outputPath) {
            return (Builder) super.outputPath(outputPath);
        }

        @Override
        public Builder jobInitialiser(final JobInitialiser jobInitialiser) {
            return (Builder) super.jobInitialiser(jobInitialiser);
        }

        @Override
        public Builder reducers(final Integer numReduceTasks) {
            return (Builder) super.reducers(numReduceTasks);
        }

        @Override
        public Builder mappers(final Integer numMapTasks) {
            return (Builder) super.mappers(numMapTasks);
        }

        @Override
        public Builder partioner(final Class<? extends Partitioner> partitioner) {
            return (Builder) super.partioner(partitioner);
        }
    }
}
