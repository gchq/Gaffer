package uk.gov.gchq.gaffer.integration.extensions;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class AwaitSpringStart implements BeforeAllCallback {

    @Override
    public void beforeAll(final ExtensionContext context) {
        // This doesn't really do anything other than force the spring context to start before any test methods are started.
    }
}
