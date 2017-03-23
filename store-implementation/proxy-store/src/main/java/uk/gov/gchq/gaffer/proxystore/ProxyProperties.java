/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.proxystore;

import org.apache.commons.lang.StringUtils;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.StoreProperties;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Properties;


public class ProxyProperties extends StoreProperties {
    public static final String GAFFER_HOST = "gaffer.host";
    public static final String GAFFER_PORT = "gaffer.port";
    public static final String GAFFER_CONTEXT_ROOT = "gaffer.context-root";
    public static final String CONNECT_TIMEOUT = "gaffer.connect-timeout";
    public static final String READ_TIMEOUT = "gaffer.read-timeout";
    public static final String JSON_SERIALISER_CLASS = "gaffer.jsonserialiser.class";

    public static final String DEFAULT_GAFFER_HOST = "localhost";
    public static final String DEFAULT_GAFFER_CONTEXT_ROOT = "/rest/v1";
    public static final int DEFAULT_GAFFER_PORT = 8080;
    public static final int DEFAULT_CONNECT_TIMEOUT = 10000;
    public static final int DEFAULT_READ_TIMEOUT = 10000;
    public static final String DEFAULT_JSON_SERIALISER_CLASS = JSONSerialiser.class.getName();

    public ProxyProperties() {
    }

    public ProxyProperties(final Path propFileLocation) {
        super(propFileLocation);
    }

    public ProxyProperties(final Properties props) {
        super(props);
    }

    public int getConnectTimeout() {
        final String timeout = get(CONNECT_TIMEOUT, null);
        try {
            return null == timeout ? DEFAULT_CONNECT_TIMEOUT : Integer.parseInt(timeout);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Unable to convert gaffer timeout into an integer", e);
        }
    }

    public void setConnectTimeout(final int timeout) {
        set(CONNECT_TIMEOUT, String.valueOf(timeout));
    }

    public int getReadTimeout() {
        final String timeout = get(READ_TIMEOUT, null);
        try {
            return null == timeout ? DEFAULT_READ_TIMEOUT : Integer.parseInt(timeout);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Unable to convert gaffer timeout into an integer", e);
        }
    }

    public void setReadTimeout(final int timeout) {
        set(READ_TIMEOUT, String.valueOf(timeout));
    }

    public String getGafferHost() {
        return get(GAFFER_HOST, DEFAULT_GAFFER_HOST);
    }

    public void setGafferHost(final String gafferHost) {
        set(GAFFER_HOST, gafferHost);
    }

    public int getGafferPort() {
        final String portStr = get(GAFFER_PORT, null);
        try {
            return null == portStr ? DEFAULT_GAFFER_PORT : Integer.parseInt(portStr);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Unable to convert gaffer port into an integer", e);
        }
    }

    public void setGafferPort(final int gafferPort) {
        set(GAFFER_PORT, String.valueOf(gafferPort));
    }

    public String getGafferContextRoot() {
        return get(GAFFER_CONTEXT_ROOT, DEFAULT_GAFFER_CONTEXT_ROOT);
    }

    public void setGafferContextRoot(final String gafferContextRoot) {
        set(GAFFER_CONTEXT_ROOT, gafferContextRoot);
    }

    public JSONSerialiser getJsonSerialiser() {
        return JSONSerialiser.fromClass(getJsonSerialiserClass());
    }

    public String getJsonSerialiserClass() {
        return get(JSON_SERIALISER_CLASS, DEFAULT_JSON_SERIALISER_CLASS);
    }

    public void setJsonSerialiserClass(final String jsonSerialiserClass) {
        set(JSON_SERIALISER_CLASS, jsonSerialiserClass);
    }

    public URL getGafferUrl() {
        return getGafferUrl(null);
    }

    public URL getGafferUrl(final String suffix) {
        return getGafferUrl("http", suffix);
    }

    public URL getGafferUrl(final String protocol, final String suffix) {
        final String urlSuffix;
        if (StringUtils.isNotEmpty(suffix)) {
            urlSuffix = prepend("/", suffix);
        } else {
            urlSuffix = "";
        }

        try {
            String contextRoot = prepend("/", getGafferContextRoot());
            contextRoot = removeSuffix("/", contextRoot);
            return new URL(protocol, getGafferHost(), getGafferPort(),
                    contextRoot + urlSuffix);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Could not create Gaffer URL from host (" + getGafferHost()
                    + "), port (" + getGafferPort()
                    + ") and context root (" + getGafferContextRoot() + ")", e);
        }
    }

    protected String removeSuffix(final String suffix, final String string) {
        if (string.endsWith(suffix)) {
            return string.substring(0, string.length() - suffix.length() - 1);
        }

        return string;
    }

    protected String prepend(final String prefix, final String string) {
        if (!string.startsWith(prefix)) {
            return prefix + string;
        }

        return string;
    }
}
