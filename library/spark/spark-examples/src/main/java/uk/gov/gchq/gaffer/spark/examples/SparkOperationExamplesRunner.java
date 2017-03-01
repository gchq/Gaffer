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
package uk.gov.gchq.gaffer.spark.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import uk.gov.gchq.gaffer.example.operation.OperationExample;
import uk.gov.gchq.gaffer.example.util.ExamplesRunner;
import uk.gov.gchq.gaffer.operation.Operation;

/**
 * This runner will run all spark operation examples.
 */
public class SparkOperationExamplesRunner extends ExamplesRunner {
    private static final Logger ROOT_LOGGER = Logger.getRootLogger();

    public static void main(final String[] args) throws Exception {
        new SparkOperationExamplesRunner().run();
    }

    public void run() throws Exception {
        run(OperationExample.class, Operation.class, "Spark operation");
        ROOT_LOGGER.setLevel(Level.OFF);
    }

    @Override
    protected void printEditWarning(final String type) {
        log("_This page has been generated from code. To make any changes please update the " + type + " examples in the [spark-examples](https://github.com/gchq/Gaffer/tree/master/library/spark/spark-examples/src/main/java/uk/gov/gchq/gaffer/spark/examples) module, run it and replace the content of this page with the output._\n\n");
    }
}
