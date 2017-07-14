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
package uk.gov.gchq.gaffer.store.util;

import org.apache.commons.io.FileUtils;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.io.IOException;

/**
 * Utility to export a Schema written in java to a json file.
 * To use you will to create an implementation of SchemaFactory where you
 * must create your schema.
 * <p>
 * Usage: java -cp jar-with-dependencies.jar uk.gov.gchq.gaffer.store.util.SchemaExporter &lt;SchemaFactory class name&gt; [file path] [y/n (override)]
 * </p>
 * <p>
 * Or trigger in a maven build profile:
 * <pre>
 * &lt;build&gt;
 *     &lt;plugins&gt;
 *         &lt;plugin&gt;
 *             &lt;groupId&gt;org.codehaus.mojo&lt;/groupId&gt;
 *             &lt;artifactId&gt;exec-maven-plugin&lt;/artifactId&gt;
 *             &lt;version&gt;1.6.0&lt;/version&gt;
 *             &lt;executions&gt;
 *                 &lt;execution&gt;
 *                     &lt;phase&gt;compile&lt;/phase&gt;
 *                     &lt;goals&gt;
 *                         &lt;goal&gt;java&lt;/goal&gt;
 *                     &lt;/goals&gt;
 *                 &lt;/execution&gt;
 *             &lt;/executions&gt;
 *             &lt;configuration&gt;
 *                 &lt;mainClass&gt;
 *                   uk.gov.gchq.gaffer.store.util.SchemaExporter
 *                 &lt;/mainClass&gt;
 *                 &lt;arguments&gt;
 *                     &lt;argument&gt;
 *                         uk.gov.gchq.gaffer.basic.BasicSchema
 *                     &lt;/argument&gt;
 *                     &lt;argument&gt;
 *                         ${project.build.directory}/schema/schema.json
 *                     &lt;/argument&gt;
 *                     &lt;argument&gt;
 *                         y
 *                     &lt;/argument&gt;
 *                 &lt;/arguments&gt;
 *             &lt;/configuration&gt;
 *         &lt;/plugin&gt;
 *     &lt;/plugins&gt;
 * &lt;/build&gt;
 * </pre>
 * </p>
 */
public class SchemaExporter {
    public static final String DEFAULT_FILE_PATH = "schema/schema.json";

    public static void main(final String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (args.length < 1) {
            System.err.println("Usage: java -cp jar-with-dependencies.jar "
                    + SchemaExporter.class.getName()
                    + " <SchemaFactory class name> [file path] [y/n (override)]");
            System.exit(1);
        }

        new SchemaExporter().run(args);
    }

    private void run(final String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        final String schemaFactoryClassName = args[0];
        final SchemaFactory schemaFactory = Class.forName(schemaFactoryClassName).asSubclass(SchemaFactory.class).newInstance();

        final String filePath;
        if (args.length > 1) {
            filePath = args[1];
        } else {
            filePath = DEFAULT_FILE_PATH;
        }

        final boolean override = args.length > 2 && "y".equalsIgnoreCase(args[2]);
        if (new File(filePath).exists()) {
            if (override) {
                FileUtils.forceDelete(new File(filePath));
            } else {
                System.err.println("Unable to write schema to file as the file " + filePath + " already exists.");
                System.exit(1);
            }
        }

        exportSchema(schemaFactory.getSchema(), filePath);
    }

    private void exportSchema(final Schema schema, final String filePath) throws IOException {
        FileUtils.write(new File(filePath), StringUtil.toString(schema.toJson(true)));
        System.out.println("Schema written to file: " + filePath);
    }
}
