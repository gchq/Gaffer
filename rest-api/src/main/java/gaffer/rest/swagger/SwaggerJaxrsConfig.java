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

package gaffer.rest.swagger;

import com.wordnik.swagger.config.ConfigFactory;
import com.wordnik.swagger.config.ScannerFactory;
import com.wordnik.swagger.config.SwaggerConfig;
import com.wordnik.swagger.jaxrs.config.ReflectiveJaxrsScanner;
import com.wordnik.swagger.jaxrs.reader.DefaultJaxrsApiReader;
import com.wordnik.swagger.reader.ClassReaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gaffer.rest.SystemProperty;

import javax.servlet.ServletConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;

/**
 * Sets up Swagger to reflectively lookup REST API methods in the package defined in the system property
 * "gaffer.rest-api.resourcePackage" - this defaults to "gaffer.rest".
 */
@WebServlet(name = "SwaggerJaxrsConfig", loadOnStartup = 2)
public class SwaggerJaxrsConfig extends HttpServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(SwaggerJaxrsConfig.class);
    private static final long serialVersionUID = 459189376294803855L;

    @Override
    public void init(final ServletConfig servletConfig) {
        try {
            super.init(servletConfig);

            ReflectiveJaxrsScanner scanner = new ReflectiveJaxrsScanner();
            scanner.setResourcePackage(System.getProperty(SystemProperty.SERVICES_PACKAGE_PREFIX, SystemProperty.SERVICES_PACKAGE_PREFIX_DEFAULT));
            ScannerFactory.setScanner(scanner);

            ClassReaders.setReader(new DefaultJaxrsApiReader());

            SwaggerConfig swaggerConfig = new SwaggerConfig();
            ConfigFactory.setConfig(swaggerConfig);

            String baseUrl = System.getProperty(SystemProperty.BASE_URL, SystemProperty.BASE_URL_DEFAULT);
            if (!baseUrl.startsWith("/")) {
                baseUrl = "/" + baseUrl;
            }
            swaggerConfig.setBasePath(baseUrl);
            swaggerConfig.setApiVersion(System.getProperty(SystemProperty.VERSION, SystemProperty.CORE_VERSION));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
