/*
 * Copyright 2017-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.spark.api.java.JavaRDD;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.0.0")
@Summary("Imports a JavaRDD of elements")
public class ImportJavaRDDOfElements implements
        Operation,
        Input<JavaRDD<Element>> {
    public static final String HADOOP_CONFIGURATION_KEY = "Hadoop_Configuration_Key";
    private JavaRDD<Element> input;
    private Map<String, String> options;

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
                .input(input)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<ImportJavaRDDOfElements, Builder>
            implements Input.Builder<ImportJavaRDDOfElements, JavaRDD<Element>, Builder>,
            Operation.Builder<ImportJavaRDDOfElements, Builder> {
        public Builder() {
            super(new ImportJavaRDDOfElements());
        }
    }
}
