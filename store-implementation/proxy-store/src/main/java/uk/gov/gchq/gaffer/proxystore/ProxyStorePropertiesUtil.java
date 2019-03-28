package uk.gov.gchq.gaffer.proxystore;

import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StorePropertiesUtil;

import java.net.MalformedURLException;
import java.net.URL;

public class ProxyStorePropertiesUtil extends StorePropertiesUtil {
    public static final String GAFFER_HOST = "gaffer.host";
    public static final String GAFFER_PORT = "gaffer.port";
    public static final String GAFFER_CONTEXT_ROOT = "gaffer.context-root";
    public static final String CONNECT_TIMEOUT = "gaffer.connect-timeout";
    public static final String READ_TIMEOUT = "gaffer.read-timeout";

    public static final String DEFAULT_GAFFER_HOST = "localhost";
    public static final String DEFAULT_GAFFER_CONTEXT_ROOT = "/rest";
    public static final int DEFAULT_GAFFER_PORT = 8080;
    public static final int DEFAULT_CONNECT_TIMEOUT = 10000;
    public static final int DEFAULT_READ_TIMEOUT = 10000;

    private static final String GAFFER_REST_API_VERSION = "v2";

    public static int getConnectTimeout(final StoreProperties proxyProperties) {
        return readInt(proxyProperties, CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
    }

    public static void setConnectTimeout(final StoreProperties proxyProperties, final int timeout) {
        proxyProperties.setProperty(CONNECT_TIMEOUT, String.valueOf(timeout));
    }

    public static int getReadTimeout(final StoreProperties proxyProperties) {
        return readInt(proxyProperties, READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
    }

    public static void setReadTimeout(final StoreProperties proxyProperties, final int timeout) {
        proxyProperties.setProperty(READ_TIMEOUT, String.valueOf(timeout));
    }

    public static String getGafferHost(final StoreProperties proxyProperties) {
        return proxyProperties.getProperty(GAFFER_HOST, DEFAULT_GAFFER_HOST);
    }

    public static void setGafferHost(final StoreProperties proxyProperties, final String gafferHost) {
        proxyProperties.setProperty(GAFFER_HOST, gafferHost);
    }

    public static int getGafferPort(final StoreProperties proxyProperties) {
        return readInt(proxyProperties, GAFFER_PORT, DEFAULT_GAFFER_PORT);
    }

    public static void setGafferPort(final StoreProperties proxyProperties, final int gafferPort) {
        proxyProperties.setProperty(GAFFER_PORT, String.valueOf(gafferPort));
    }

    public static String getGafferContextRoot(final StoreProperties proxyProperties) {
        return proxyProperties.getProperty(GAFFER_CONTEXT_ROOT, DEFAULT_GAFFER_CONTEXT_ROOT);
    }

    public static void setGafferContextRoot(final StoreProperties proxyProperties, final String gafferContextRoot) {
        final String checkedGafferContextRoot;
        if (!gafferContextRoot.startsWith("/")) {
            checkedGafferContextRoot = "/" + gafferContextRoot;
        } else {
            checkedGafferContextRoot = gafferContextRoot;
        }
        proxyProperties.setProperty(GAFFER_CONTEXT_ROOT, checkedGafferContextRoot);
    }

    public static URL getGafferUrl(final StoreProperties proxyProperties) {
        return getGafferUrl(proxyProperties, null);
    }

    public static URL getGafferUrl(final StoreProperties proxyProperties, final String suffix) {
        return getGafferUrl(proxyProperties, "http", suffix);
    }

    public static URL getGafferUrl(final StoreProperties proxyProperties, final String protocol, final String suffix) {
        final String urlSuffix;
        if (StringUtils.isNotEmpty(suffix)) {
            urlSuffix = prepend("/", suffix);
        } else {
            urlSuffix = "";
        }

        try {
            String contextRoot = prepend("/", getGafferContextRoot(proxyProperties));
            contextRoot = addSuffix("/", contextRoot) + GAFFER_REST_API_VERSION;
            return new URL(protocol, getGafferHost(proxyProperties), getGafferPort(proxyProperties),
                    contextRoot + urlSuffix);
        } catch (final MalformedURLException e) {
            throw new IllegalArgumentException("Could not create Gaffer URL from host (" + getGafferHost(proxyProperties)
                    + "), port (" + getGafferPort(proxyProperties)
                    + ") and context root (" + getGafferContextRoot(proxyProperties) + ")", e);
        }
    }

    protected static String addSuffix(final String suffix, final String string) {
        if (!string.endsWith(suffix)) {
            return string + suffix;
        }

        return string;
    }

    protected static String prepend(final String prefix, final String string) {
        if (!string.startsWith(prefix)) {
            return prefix + string;
        }

        return string;
    }

    private static int readInt(final StoreProperties proxyProperties, final String propName, final int defaultValue) {
        final String property = proxyProperties.getProperty(propName, null);
        try {
            return null == property ? defaultValue : Integer.parseInt(property);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Unable to convert %s into an integer", propName), e);
        }
    }
}
