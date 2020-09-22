/*
 * Copyright 2016-2020 Crown Copyright
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

import com.fasterxml.classmate.TypeResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.schema.AlternateTypeRules;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import uk.gov.gchq.gaffer.operation.Operation;

import java.util.Set;

import static springfox.documentation.builders.PathSelectors.regex;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE_DEFAULT;

@EnableSwagger2
@Configuration
public class SwaggerConfig {

    @Autowired
    private TypeResolver typeResolver;

    @Bean
    public ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title(System.getProperty(APP_TITLE, APP_TITLE_DEFAULT))
                .description(System.getProperty(APP_DESCRIPTION, APP_DESCRIPTION_DEFAULT))
                .version("3.0-alpha")
                .build();
    }

    @Bean
    public Docket gafferApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("uk.gov.gchq.gaffer.rest"))
                .paths(regex("/.*"))
                .build()
                .apiInfo(apiInfo())
                .alternateTypeRules(
                        AlternateTypeRules.newRule(
                                typeResolver.resolve(Set.class, Class.class),
                                typeResolver.resolve(Set.class, String.class)
                        ),
                        AlternateTypeRules.newRule(
                                typeResolver.resolve(Set.class, typeResolver.resolve(Class.class, Operation.class)),
                                typeResolver.resolve(Set.class, String.class)
                        )
                );
    }
}
