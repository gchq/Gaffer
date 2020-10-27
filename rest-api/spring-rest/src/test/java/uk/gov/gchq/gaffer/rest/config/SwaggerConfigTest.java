package uk.gov.gchq.gaffer.rest.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import springfox.documentation.service.ApiInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE_DEFAULT;

public class SwaggerConfigTest {

    @BeforeEach
    public void clearProperty() {
        System.clearProperty(APP_TITLE);
    }

    @Test
    public void shouldPullTitleFromSystemProperty() {
        // Given
        final String title = "My Gaffer Graph";
        SwaggerConfig swaggerConfig = new SwaggerConfig();
        swaggerConfig.setEnvironment(new StandardEnvironment());
        System.setProperty(APP_TITLE, title);

        // When
        ApiInfo apiInfo = swaggerConfig.apiInfo();

        // Then
        assertEquals(title, apiInfo.getTitle());
    }

    @Test
    public void shouldPullTitleFromEnvironment() {
        // Given
        final String title = "My Gaffer Graph";
        SwaggerConfig swaggerConfig = new SwaggerConfig();
        Environment env = mock(Environment.class);

        when(env.getProperty(APP_TITLE, APP_TITLE_DEFAULT)).thenReturn(title);

        // When
        swaggerConfig.setEnvironment(env);
        ApiInfo apiInfo = swaggerConfig.apiInfo();

        // Then
        assertEquals(title, apiInfo.getTitle());
    }

    @Test
    public void shouldPullDescriptionFromSystemProperty() {
        // Given
        final String description = "My Gaffer Graph";
        SwaggerConfig swaggerConfig = new SwaggerConfig();
        swaggerConfig.setEnvironment(new StandardEnvironment());
        System.setProperty(APP_DESCRIPTION, description);

        // When
        ApiInfo apiInfo = swaggerConfig.apiInfo();

        // Then
        assertEquals(description, apiInfo.getDescription());
    }

    @Test
    public void shouldPullDescriptionFromEnvironment() {
        // Given
        final String description = "My Gaffer Graph";
        SwaggerConfig swaggerConfig = new SwaggerConfig();
        Environment env = mock(Environment.class);

        when(env.getProperty(APP_DESCRIPTION, APP_DESCRIPTION_DEFAULT)).thenReturn(description);

        // When
        swaggerConfig.setEnvironment(env);
        ApiInfo apiInfo = swaggerConfig.apiInfo();

        // Then
        assertEquals(description, apiInfo.getDescription());
    }
}
