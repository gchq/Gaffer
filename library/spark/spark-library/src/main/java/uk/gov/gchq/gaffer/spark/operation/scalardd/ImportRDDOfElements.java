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
package uk.gov.gchq.gaffer.spark.operation.scalardd;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.io.Input;
import java.util.Map;

public class ImportRDDOfElements implements
        Operation,
        Input<RDD<Element>>,
        Rdd,
        Options {
    public static final String HADOOP_CONFIGURATION_KEY = "Hadoop_Configuration_Key";
    @Required
    private SparkSession sparkSession;
    private RDD<Element> input;
    private Map<String, String> options;

    @Override
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    @Override
    public void setSparkSession(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public RDD<Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final RDD<Element> input) {
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

    public static class Builder extends Operation.BaseBuilder<ImportRDDOfElements, Builder>
            implements Input.Builder<ImportRDDOfElements, RDD<Element>, Builder>,
            Rdd.Builder<ImportRDDOfElements, Builder>,
            Options.Builder<ImportRDDOfElements, Builder> {
        public Builder() {
            super(new ImportRDDOfElements());
        }
    }
}
