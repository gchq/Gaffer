package uk.gov.gchq.gaffer.integration.impl;


import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.generator.NeptuneFormat;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntityIdExtractor;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.CsvToElements;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.imprt.localfile.ImportFromLocalFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class ImportExportIT extends AbstractStoreIT {

    private static final String FILE_PATH = ImportExportIT.class.getResource("/NeptuneEntitiesAndEdgesWithProperties.csv").getPath();
    @Test
    public void shouldExportResultsInSet() throws OperationException {
        // Given

        final OperationChain<Iterable<?>> exportOpChain = new OperationChain.Builder()
                .first(new ImportFromLocalFile.Builder()
                        .input(FILE_PATH)
                        .build())
                .then(new CsvToElements.Builder()
                        .csvFormat(new NeptuneFormat())
                        .build())
                .then(new AddElements())
                .build();

        // When
        final Iterable<?> export = graph.execute(exportOpChain, getUser());

        // Then
        assertThat(Sets.newHashSet(export)).hasSize(2);
    }



}
