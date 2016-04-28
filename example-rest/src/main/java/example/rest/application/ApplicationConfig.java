/*
 * Copyright 2016 Crown Copyright
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
package example.rest.application;

import example.rest.serialisation.RestJsonProvider;
import example.rest.service.SimpleExamplesService2;
import gaffer.rest.application.AbstractApplicationConfig;
import gaffer.rest.service.SimpleExamplesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.ApplicationPath;

@ApplicationPath("v1")
public class ApplicationConfig extends AbstractApplicationConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationConfig.class);


    public ApplicationConfig() {
        super();
        resources.remove(SimpleExamplesService.class);
        resources.add(SimpleExamplesService2.class);
    }

    @Override
    protected void addSystemResources() {
        super.addSystemResources();
        resources.add(RestJsonProvider.class);
    }
}
