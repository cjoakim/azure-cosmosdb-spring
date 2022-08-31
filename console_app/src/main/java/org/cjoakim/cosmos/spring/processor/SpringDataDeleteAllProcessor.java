package org.cjoakim.cosmos.spring.processor;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.repository.TelemetryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This ConsoleAppProcessor is used to delete all documents from a given container.
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Component
@Data
@NoArgsConstructor
@Slf4j
public class SpringDataDeleteAllProcessor extends ConsoleAppProcessor implements AppConstants {

    private String container;
    long startMillis = System.currentTimeMillis();
    long loopNumber = 0;
    long documentCount = -1;
    long exceptionCount = -1;
    long initialDocumentCount = 0;
    boolean continueToProcess = true;

    @Autowired
    private TelemetryRepository telemetryRepository = null;

    public void process() throws Exception {

        log.warn("process, container:  " + container);

        if (container.equalsIgnoreCase("telemetry")) {
            deleteTelemetry();
        }
    }

    private void deleteTelemetry() throws Exception {
        log.warn("deleteTelemetry...");
        startMillis = System.currentTimeMillis();

        while (continueToProcess) {
            loopNumber++;
            documentCount = telemetryRepository.countAllDocuments();
            log.warn("loop number " + loopNumber + " document count: " + formattedCount(documentCount));
            if (loopNumber == 1) {
                initialDocumentCount = documentCount;
            }
            if (documentCount > 0) {
                try {
                    telemetryRepository.deleteAll();
                }
                catch(Throwable t) {
                    exceptionCount++;
                    t.printStackTrace();
                }
            }
            else {
                continueToProcess = false;
            }
            if (loopNumber > 100) {
                continueToProcess = false;
            }
        }
        documentCount = telemetryRepository.countAllDocuments();
        long docsDeletedCount = initialDocumentCount - documentCount;

        long elapsedMillis = System.currentTimeMillis() - startMillis;
        double elapsedMinutes = (double) elapsedMillis / 60000.0;
        log.warn("container:                " + container);
        log.warn("loops:                    " + loopNumber);
        log.warn("initial document count:   " + initialDocumentCount);
        log.warn("documents deleted:        " + docsDeletedCount);
        log.warn("remaining document count: " + documentCount);
        log.warn("exception count:          " + exceptionCount);
        log.warn("elapsed ms:               " + elapsedMillis);
        log.warn("elapsed minutes:          " + elapsedMinutes);
        log.warn("docs per minute:          " + ((double) docsDeletedCount) / elapsedMinutes);
    }
}
