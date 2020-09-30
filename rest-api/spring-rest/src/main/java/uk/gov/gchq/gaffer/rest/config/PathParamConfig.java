package uk.gov.gchq.gaffer.rest.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * This class stops the app interpreting /path/service/some.class.name as /path/service/some.class with Content Type
 * of name.
 *
 * This is necessary because quite a few endpoints in Gaffer's REST API use this mechanism.
 */
@EnableWebMvc
@Configuration
public class PathParamConfig extends WebMvcConfigurerAdapter {
    @Override
    public void configurePathMatch(final PathMatchConfigurer configurer) {
        configurer.setUseSuffixPatternMatch(false);
    }

    @Override
    public void configureContentNegotiation(final ContentNegotiationConfigurer configurer) {
        configurer.favorPathExtension(false);
    }
}
