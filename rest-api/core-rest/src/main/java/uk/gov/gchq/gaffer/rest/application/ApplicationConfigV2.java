/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.application;

import io.swagger.jaxrs.config.BeanConfig;

import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.service.v2.GraphConfigurationServiceV2;
import uk.gov.gchq.gaffer.rest.service.v2.JobServiceV2;
import uk.gov.gchq.gaffer.rest.service.v2.OperationServiceV2;
import uk.gov.gchq.gaffer.rest.service.v2.PropertiesServiceV2;
import uk.gov.gchq.gaffer.rest.service.v2.StatusServiceV2;
import uk.gov.gchq.gaffer.rest.service.v2.VersionServiceV2;
import uk.gov.gchq.gaffer.rest.service.v2.example.ExampleBinder;
import uk.gov.gchq.gaffer.rest.service.v2.example.ExamplesServiceV2;

import javax.ws.rs.Path;

import static uk.gov.gchq.gaffer.rest.application.ApplicationConfigV2.VERSION;

/**
 * An implementation of {@code ApplicationConfig}, containing v2-specific configuration for the application.
 *
 */
@Path(VERSION)
public class ApplicationConfigV2 extends ApplicationConfig {

    static final String VERSION = "v2";

    public ApplicationConfigV2() {
        super();
        register(new ExampleBinder());
    }

    @Override
    protected void setupBeanConfig() {
        final BeanConfig beanConfig = new BeanConfig();

        String basePath = System.getProperty(SystemProperty.BASE_PATH, SystemProperty.BASE_PATH_DEFAULT);
        if (basePath.length() > 0 && !basePath.startsWith("/")) {
            basePath = "/" + basePath;
        }

        beanConfig.setBasePath(basePath + '/' + VERSION);

        beanConfig.setConfigId(VERSION);
        beanConfig.setScannerId(VERSION);

        beanConfig.setResourcePackage("uk.gov.gchq.gaffer.rest.service.v2");
        beanConfig.setScan(true);

    }

    @Override
    protected void addServices() {
        resources.add(StatusServiceV2.class);
        resources.add(OperationServiceV2.class);
        resources.add(GraphConfigurationServiceV2.class);
        resources.add(JobServiceV2.class);
        resources.add(ExamplesServiceV2.class);
        resources.add(PropertiesServiceV2.class);
        resources.add(VersionServiceV2.class);
    }

}
