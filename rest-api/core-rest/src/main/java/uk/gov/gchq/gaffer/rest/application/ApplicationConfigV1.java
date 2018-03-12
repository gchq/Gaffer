/*
 * Copyright 2016-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.rest.service.v1.GraphConfigurationService;
import uk.gov.gchq.gaffer.rest.service.v1.JobService;
import uk.gov.gchq.gaffer.rest.service.v1.OperationService;
import uk.gov.gchq.gaffer.rest.service.v1.StatusService;
import uk.gov.gchq.gaffer.rest.service.v1.example.ExamplesService;

import javax.ws.rs.Path;

import static uk.gov.gchq.gaffer.rest.application.ApplicationConfigV1.VERSION;

/**
 * An implementation of {@code ApplicationConfig}, containing v1-specific configuration for the application.
 */
@Path(VERSION)
public class ApplicationConfigV1 extends ApplicationConfig {

    static final String VERSION = "v1";

    @Override
    protected void setupBeanConfig() {
        final BeanConfig beanConfig = new BeanConfig();

        String basePath = System.getProperty(SystemProperty.BASE_PATH, SystemProperty.BASE_PATH_DEFAULT);
        if (!basePath.startsWith("/")) {
            basePath = "/" + basePath;
        }

        beanConfig.setBasePath(basePath + '/' + VERSION);

        beanConfig.setConfigId(VERSION);
        beanConfig.setScannerId(VERSION);

        beanConfig.setResourcePackage("uk.gov.gchq.gaffer.rest.service.v1");
        beanConfig.setScan(true);
    }

    @Override
    protected void addServices() {
        resources.add(StatusService.class);
        resources.add(JobService.class);
        resources.add(OperationService.class);
        resources.add(GraphConfigurationService.class);
        resources.add(ExamplesService.class);
    }

}
