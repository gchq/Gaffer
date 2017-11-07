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

package uk.gov.gchq.gaffer.rest.service.v2;

import io.swagger.annotations.Contact;
import io.swagger.annotations.ExternalDocs;
import io.swagger.annotations.Info;
import io.swagger.annotations.License;
import io.swagger.annotations.SwaggerDefinition;

import uk.gov.gchq.gaffer.rest.ServiceConstants;
import uk.gov.gchq.gaffer.rest.SystemProperty;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * Empty interface containing the Swagger API definitions for the v2 Gaffer REST
 * API.
 */
@SwaggerDefinition(
        info = @Info(
                description = "",
                version = "v2",
                title = "",
                contact = @Contact(
                        name = SystemProperty.APP_CONTACT_DEFAULT,
                        url = SystemProperty.APP_CONTACT_URL_DEFAULT
                ),
                license = @License(
                        name = "Apache 2.0",
                        url = "http://www.apache.org/licenses/LICENSE-2.0"
                )
        ),
        consumes = {APPLICATION_JSON},
        produces = {APPLICATION_JSON},
        schemes = {SwaggerDefinition.Scheme.HTTP, SwaggerDefinition.Scheme.HTTPS},
        externalDocs = @ExternalDocs(value = "Documentation", url = ServiceConstants.GAFFER_DOCUMENTATION_URL)
)
public interface SwaggerDefinitionConfigV2 {
    // Empty marker interface
}
