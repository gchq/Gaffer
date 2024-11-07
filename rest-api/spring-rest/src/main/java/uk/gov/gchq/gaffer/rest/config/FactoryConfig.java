/*
 * Copyright 2020-2024 Crown Copyright
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;

import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.factory.DefaultExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.ExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.UnknownUserFactory;

import javax.annotation.PostConstruct;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Configuration
public class FactoryConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(FactoryConfig.class);

    public static final String USER_FACTORY_CLASS_DEFAULT = UnknownUserFactory.class.getName();

    private Environment environment;

    @Autowired
    public void setEnvironment(final Environment environment) {
        this.environment = environment;
    }

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
                    LOGGER.info("Skipping Property source {}", propertySource);
                    LOGGER.info("Any gaffer property configured with this source will not be automatically added " +
                            "to system properties");
                }
            });
            return;
        }
        LOGGER.warn("Environment was not instance of AbstractEnvironment. Property setting will be skipped.");
    }

    @Bean
    public GraphFactory createGraphFactory() throws IllegalAccessException, InstantiationException {
        return getDefaultGraphFactory().newInstance();
    }

    @Bean
    public AbstractUserFactory createUserFactory() throws IllegalAccessException, InstantiationException {
        return getDefaultUserFactory().newInstance();
    }

    @Bean
    public ExamplesFactory createExamplesFactory() {
        return new DefaultExamplesFactory();
    }

    private Class<? extends GraphFactory> getDefaultGraphFactory() {
        final String graphFactoryClass = System.getProperty(SystemProperty.GRAPH_FACTORY_CLASS,
                SystemProperty.GRAPH_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(graphFactoryClass)
                    .asSubclass(GraphFactory.class);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create graph factory from class: " + graphFactoryClass, e);
        }
    }

    private Class<? extends AbstractUserFactory> getDefaultUserFactory() {
        final String userFactoryClass = System.getProperty(SystemProperty.USER_FACTORY_CLASS, USER_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(userFactoryClass)
                    .asSubclass(AbstractUserFactory.class);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create user factory from class: " + userFactoryClass, e);
        }
    }
}
