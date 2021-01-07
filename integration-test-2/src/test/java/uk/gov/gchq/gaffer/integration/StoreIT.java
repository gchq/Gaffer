package uk.gov.gchq.gaffer.integration;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import uk.gov.gchq.gaffer.rest.GafferWebApplication;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;

@SpringBootTest(classes = GafferWebApplication.class, webEnvironment = DEFINED_PORT)
@ActiveProfiles("proxy")
public interface StoreIT {
}
