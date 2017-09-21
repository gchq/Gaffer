/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v1;

import io.swagger.annotations.Contact;
import io.swagger.annotations.ExternalDocs;
import io.swagger.annotations.Info;
import io.swagger.annotations.License;
import io.swagger.annotations.SwaggerDefinition;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * Empty interface containing the Swagger API definitions for the v1 Gaffer REST
 * API.
 */
@SwaggerDefinition(
        info = @Info(
                description = "The Gaffer REST service.",
                version = "v1",
                title = "Gaffer REST API",
                contact = @Contact(
                        name = "Gaffer Developers",
                        url = "https://github.com/gchq/Gaffer"
                ),
                license = @License(
                        name = "Apache 2.0",
                        url = "http://www.apache.org/licenses/LICENSE-2.0"
                )
        ),
        consumes = {APPLICATION_JSON},
        produces = {APPLICATION_JSON},
        schemes = {SwaggerDefinition.Scheme.HTTP, SwaggerDefinition.Scheme.HTTPS},
        externalDocs = @ExternalDocs(value = "Wiki", url = "https://github.com/gchq/Gaffer/wiki")
)
public interface SwaggerDefinitionConfig {
    // Empty marker interface
}
