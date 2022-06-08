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

import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.GAFFER_VERSION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.GAFFER_VERSION_DEFAULT;

@Configuration
public class SwaggerConfig {

    @Autowired
    private Environment environment;

    @Bean
    public OpenAPI apiInfo() {
        return new OpenAPI()
                .info(new Info()
                        .title(environment.getProperty(APP_TITLE, APP_TITLE_DEFAULT))
                        .description(environment.getProperty(APP_DESCRIPTION, APP_DESCRIPTION_DEFAULT))
                        .version(environment.getProperty(GAFFER_VERSION, GAFFER_VERSION_DEFAULT))
                        .license(new License()
                                .name("Apache 2.0")));
    }
}
