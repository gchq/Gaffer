package uk.gov.gchq.gaffer.hbasestore.integration.impl.loader;

import org.junit.jupiter.api.Disabled;

import uk.gov.gchq.gaffer.hbasestore.integration.HbaseStoreTest;
import uk.gov.gchq.gaffer.hdfs.integration.loader.AddElementsFromHdfsLoaderITTemplate;
import uk.gov.gchq.gaffer.integration.extensions.LoaderTestCase;

@HbaseStoreTest
public class AddElementsFromHdfsLoaderIT extends AddElementsFromHdfsLoaderITTemplate {
    @Override
    @Disabled // Known issue that directory is not empty
    public void shouldAddElementsFromHdfsWhenDirectoriesAlreadyExist(final LoaderTestCase testCase) throws Exception {
    }

    @Override
    @Disabled // Known issue when Failure directory exists
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenFailureDirectoryContainsFiles(final LoaderTestCase testCase) throws Exception {

    }
}
