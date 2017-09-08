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

package uk.gov.gchq.gaffer.traffic.generator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Logger;

public class RoadTrafficDataLoader {

    private static final Logger LOGGER = Logger.getLogger(RoadTrafficDataLoader.class.getName());

    private final Graph graph;
    private final User user;

    public RoadTrafficDataLoader(final Graph graph, final User user) {
        this.graph = graph;
        this.user = user;
    }

    public void load(final String csvString) throws IOException, OperationException {
        try (final StringReader reader = new StringReader(csvString)) {
            load(reader);
        }
    }

    public void load(final File dataFile) throws IOException, OperationException {
        try (final FileReader reader = new FileReader(dataFile)) {
            load(reader);
        }
    }

    public void load(final Reader reader) throws OperationException, IOException {
        try (final CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            final OperationChain<Void> populateChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<CSVRecord>()
                    .input(parser)
                    .generator(new RoadTrafficCsvElementGenerator())
                    .build())
                .then(new AddElements.Builder()
                    .skipInvalidElements(false)
                    .build())
                .build();

            this.graph.execute(populateChain, this.user);
        }
    }

    public static void main(final String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: " + RoadTrafficDataLoader.class.getSimpleName() + " <graphConfigFile> <schemaDir> <storePropsFile> <roadTrafficDataFile.csv>");
            System.exit(1);
        }

        final String graphConfigFile = args[0];
        final String schemaDir = args[1];
        final String storePropertiesFile = args[2];
        final String dataFile = args[3];

        final GraphConfig config = new GraphConfig.Builder().json(new File(graphConfigFile).toPath()).build();
        final Schema schema = Schema.fromJson(new File(schemaDir).toPath());
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(storePropertiesFile);

        final Graph graph = new Graph.Builder()
            .config(config)
            .addSchemas(schema)
            .storeProperties(storeProperties)
            .build();

        final User user = new User();

        final RoadTrafficDataLoader dataLoader = new RoadTrafficDataLoader(graph, user);

        LOGGER.info("Loading data");
        try {
            dataLoader.load(new File(dataFile));
            LOGGER.info("Data has been loaded");
        } catch (final Exception e) {
            LOGGER.info("Unable to load data: " + e.getMessage());
            throw new RuntimeException("Unable to load data", e);
        }
    }

}
