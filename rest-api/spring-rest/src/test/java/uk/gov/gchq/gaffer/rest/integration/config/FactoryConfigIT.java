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

package uk.gov.gchq.gaffer.rest.integration.config;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockUserFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.integration.controller.AbstractRestApiIT;

import static org.assertj.core.api.Assertions.assertThat;
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class FactoryConfigIT extends AbstractRestApiIT {

    @Autowired
    GraphFactory graphFactory;

    @Autowired
    UserFactory userFactory;

    @BeforeClass()
    public static void setSystemProperty() {
        System.setProperty(SystemProperty.USER_FACTORY_CLASS, MockUserFactory.class.getName());
    }

    @AfterClass
    public static void clearSystemProperty() {
        System.clearProperty(SystemProperty.USER_FACTORY_CLASS);
    }

    @Test
    public void shouldUseGraphFactoryDefinedInApplicationProperties() {
        assertThat(graphFactory.getClass()).isEqualTo(MockGraphFactory.class);
    }

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.BEFORE_METHOD)
    public void shouldUseSystemPropertyToDefineFactories() {
        assertThat(userFactory.getClass()).isEqualTo(MockUserFactory.class);
    }
}
