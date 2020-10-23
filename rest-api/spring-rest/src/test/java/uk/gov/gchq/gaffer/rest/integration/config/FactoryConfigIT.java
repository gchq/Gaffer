package uk.gov.gchq.gaffer.rest.integration.config;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;

import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.MockUserFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.integration.controller.AbstractRestApiIT;

import static org.junit.Assert.assertEquals;
public class FactoryConfigIT extends AbstractRestApiIT {

    @Autowired
    GraphFactory graphFactory;

    @Autowired
    UserFactory userFactory;

    @BeforeClass()
    public static void setSystemProperty() {
        System.setProperty(SystemProperty.USER_FACTORY_CLASS, MockUserFactory.class.getName());
    }

    @Test
    public void shouldUseGraphFactoryDefinedInApplicationProperties() {
        assertEquals(MockGraphFactory.class, graphFactory.getClass());
    }

    @Test
    public void shouldUseSystemPropertyToDefineFactories() {
        assertEquals(MockUserFactory.class, userFactory.getClass());
    }
}
