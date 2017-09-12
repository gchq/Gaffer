/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.spark.operation.javardd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.io.Input;

import java.util.Map;

/**
 * A {@code ImportJavaRDDOfElements} takes a {@link JavaRDD} containing {@link Element}s
 * and adds them to a target Gaffer store.
 */
public class ImportJavaRDDOfElements implements
        Input<JavaRDD<Element>>,
        JavaRdd {
    public static final String HADOOP_CONFIGURATION_KEY = "Hadoop_Configuration_Key";
    @Required
    private JavaSparkContext sparkContext;
    private JavaRDD<Element> input;
    private Map<String, String> options;

    @Override
    public JavaSparkContext getJavaSparkContext() {
        return sparkContext;
    }

    @Override
    public void setJavaSparkContext(final JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    @Override
    public JavaRDD<Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final JavaRDD<Element> input) {
        this.input = input;
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
    public ImportJavaRDDOfElements shallowClone() {
        return new ImportJavaRDDOfElements.Builder()
                .javaSparkContext(sparkContext)
                .input(input)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<ImportJavaRDDOfElements, Builder>
            implements Input.Builder<ImportJavaRDDOfElements, JavaRDD<Element>, Builder>,
            JavaRdd.Builder<ImportJavaRDDOfElements, Builder> {
        public Builder() {
            super(new ImportJavaRDDOfElements());
        }
    }
}
