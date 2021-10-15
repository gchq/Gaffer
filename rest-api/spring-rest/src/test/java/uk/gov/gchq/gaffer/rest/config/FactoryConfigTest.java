/*
 * Copyright 2020-2021 Crown Copyright
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.mock.env.MockPropertySource;

import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockUserFactory;
import uk.gov.gchq.gaffer.rest.factory.UnknownUserFactory;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.rest.SystemProperty.GRAPH_FACTORY_CLASS;
import static uk.gov.gchq.gaffer.rest.SystemProperty.USER_FACTORY_CLASS;

public class FactoryConfigTest {

    @BeforeEach
    @AfterEach
    public void clearSystemProperties() {
        System.clearProperty(GRAPH_FACTORY_CLASS);
        System.clearProperty(USER_FACTORY_CLASS);
    }


    @Test
    public void shouldUseSystemPropertiesToCreateGraphFactory() throws InstantiationException, IllegalAccessException {
        // Given
        System.setProperty(GRAPH_FACTORY_CLASS, MockGraphFactory.class.getName());

        // When
        FactoryConfig factoryConfig = new FactoryConfig();

        // Then
        assertEquals(factoryConfig.createGraphFactory().getClass(), MockGraphFactory.class);
    }

    @Test
    public void shouldUseSystemPropertiesToCreateUserFactory() throws InstantiationException, IllegalAccessException {
        // Given
        System.setProperty(USER_FACTORY_CLASS, MockUserFactory.class.getName());

        // When
        FactoryConfig factoryConfig = new FactoryConfig();

        // Then
        assertEquals(factoryConfig.createUserFactory().getClass(), MockUserFactory.class);
    }

    @Test
    public void shouldUseDefaultGraphFactoryByDefault() throws InstantiationException, IllegalAccessException {
        // Given - no system properties

        // When
        FactoryConfig factoryConfig = new FactoryConfig();

        // Then
        assertEquals(factoryConfig.createGraphFactory().getClass(), DefaultGraphFactory.class);
    }

    @Test
    public void shouldUseUnknownUserFactoryByDefault() throws InstantiationException, IllegalAccessException {
        // Given - no system properties

        // When
        FactoryConfig factoryConfig = new FactoryConfig();

        // Then
        assertEquals(factoryConfig.createUserFactory().getClass(), UnknownUserFactory.class);
    }

    @Test
    public void shouldUsePropertiesFromEnvironmentToSetUpGraphFactory() throws InstantiationException, IllegalAccessException {
        // Given
        FactoryConfig factoryConfig = new FactoryConfig();
        AbstractEnvironment mockEnv = mock(AbstractEnvironment.class);

        Properties properties = new Properties();
        properties.setProperty(GRAPH_FACTORY_CLASS, MockGraphFactory.class.getName());
        MockPropertySource mockPropertySource = new MockPropertySource(properties);

        MutablePropertySources propertySources = new MutablePropertySources();
        propertySources.addFirst(mockPropertySource);

        when(mockEnv.getPropertySources()).thenReturn(propertySources);
        when(mockEnv.getProperty(GRAPH_FACTORY_CLASS)).then(invocation -> mockPropertySource.getProperty(invocation.getArgumentAt(0, String.class)));

        factoryConfig.setEnvironment(mockEnv); // called by spring normally

        // When
        factoryConfig.setToSystemProperties(); // Called by spring

        // Then
        assertEquals(MockGraphFactory.class, factoryConfig.createGraphFactory().getClass());
    }

    @Test
    public void shouldUsePropertiesFromEnvironmentToSetUpUserFactory() throws InstantiationException, IllegalAccessException {
        // Given
        FactoryConfig factoryConfig = new FactoryConfig();
        AbstractEnvironment mockEnv = mock(AbstractEnvironment.class);

        Properties properties = new Properties();
        properties.setProperty(USER_FACTORY_CLASS, MockUserFactory.class.getName());
        MockPropertySource mockPropertySource = new MockPropertySource(properties);

        MutablePropertySources propertySources = new MutablePropertySources();
        propertySources.addFirst(mockPropertySource);

        when(mockEnv.getPropertySources()).thenReturn(propertySources);
        when(mockEnv.getProperty(USER_FACTORY_CLASS)).then(invocation -> mockPropertySource.getProperty(invocation.getArgumentAt(0, String.class)));

        factoryConfig.setEnvironment(mockEnv); // called by spring normally

        // When
        factoryConfig.setToSystemProperties(); // Called by spring

        // Then
        assertEquals(MockUserFactory.class, factoryConfig.createUserFactory().getClass());
    }

    @Test
    public void shouldThrowExceptionWhenFactoryIsTheWrongClass() {
        // Given
        System.setProperty(GRAPH_FACTORY_CLASS, MockUserFactory.class.getName());

        // When
        FactoryConfig factoryConfig = new FactoryConfig();

        // Then
        assertThatExceptionOfType(ClassCastException.class)
                .isThrownBy(factoryConfig::createGraphFactory)
                .extracting(Throwable::getMessage)
                .isNotNull();
    }
}
