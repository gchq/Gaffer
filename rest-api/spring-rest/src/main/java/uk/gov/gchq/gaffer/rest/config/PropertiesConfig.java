/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Gaffer uses system properties across the app to provide configuration.
 * This class escalates any gaffer properties to system properties that may be passed to the
 * app via Command line args eg:
 *
 * {@code java -jar spring-rest.jar --gaffer.app.name=MyAppName}
 *
 * Or via properties specified in the application.properties file.
 */
@Configuration
public class PropertiesConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesConfig.class);

    @Autowired
    private Environment environment;

    @PostConstruct
    public void setToSystemProperties() {
        if (environment instanceof AbstractEnvironment) {
            Set<String> checkedProperties = new HashSet<>();

            ((AbstractEnvironment) environment).getPropertySources().iterator().forEachRemaining(propertySource -> {
                if (propertySource instanceof EnumerablePropertySource) {
                    String[] propertyNames = ((EnumerablePropertySource) propertySource).getPropertyNames();
                    Arrays.stream(propertyNames)
                            .filter(s -> s.startsWith("gaffer") && checkedProperties.add(s)) // Skips expensive property lookup
                            .forEach(s -> System.setProperty(s, environment.getProperty(s)));
                } else {
                    LOGGER.info("Skipping Property source " + propertySource);
                    LOGGER.info("Any gaffer property configured with this source will not be automatically added " +
                            "to system properties");
                }
            });
            return;
        }

        LOGGER.warn("Environment was not instance of AbstractEnvironment. Property setting will be skipped.");
    }
}
