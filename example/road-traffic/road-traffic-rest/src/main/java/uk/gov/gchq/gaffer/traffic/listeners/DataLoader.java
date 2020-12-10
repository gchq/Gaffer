///*
// * Copyright 2016-2020 Crown Copyright
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package uk.gov.gchq.gaffer.traffic.listeners;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.core.env.Environment;
//import org.springframework.stereotype.Component;
//
//import uk.gov.gchq.gaffer.graph.Graph;
//import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
//import uk.gov.gchq.gaffer.traffic.generator.RoadTrafficDataLoader;
//import uk.gov.gchq.gaffer.user.User;
//
//import javax.annotation.PostConstruct;
//import javax.servlet.ServletContextListener;
//
//import java.io.File;
//import java.util.logging.Logger;
//
///**
// * A {@link ServletContextListener} to load the road traffic dataset into the application
// * automatically upon application startup.
// */
//@Component
//public class DataLoader {
//    public static final String DATA_PATH = "roadTraffic.dataLoader.dataPath";
//
//    private static final Logger LOGGER = Logger.getLogger(DataLoader.class.getName());
//
//    private Environment environment;
//
//    @Autowired
//    public DataLoader(final Environment environment) {
//        this.environment = environment;
//    }
//
//    @PostConstruct
//    public void contextInitialized() {
//        final String dataPath = environment.getProperty(DATA_PATH);
//        if (null != dataPath) {
//            loadData(dataPath);
//        }
//    }
//
//    private void loadData(final String dataPath) {
//        LOGGER.info("Loading data");
//
//        final Graph graph = GraphFactory.createGraphFactory().getGraph();
//
//        final RoadTrafficDataLoader dataLoader = new RoadTrafficDataLoader(graph, new User());
//        try {
//            dataLoader.load(new File(dataPath));
//        } catch (final Exception e) {
//            LOGGER.info("Unable to load data: " + e.getMessage());
//            throw new RuntimeException("Unable to load data", e);
//        }
//
//        LOGGER.info("Sample data has been loaded");
//    }
//}
