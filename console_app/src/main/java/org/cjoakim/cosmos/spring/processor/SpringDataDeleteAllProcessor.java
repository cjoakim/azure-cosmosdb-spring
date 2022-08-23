package org.cjoakim.cosmos.spring.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.model.EpaOzoneTelemetryEvent;
import org.cjoakim.cosmos.spring.repository.EpaOzoneTelemetryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 *
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Component
@Data
@NoArgsConstructor
@Slf4j
public class SpringDataDeleteAllProcessor extends ConsoleAppProcessor implements AppConstants {

    private String container;

    @Autowired
    private EpaOzoneTelemetryRepository epaOzoneTelemetryRepository = null;

    public void process() throws Exception {

        log.warn("process, container:  " + container);

        if (container.equalsIgnoreCase("telemetry")) {
            deleteTelemetry();
        }
    }

    private void deleteTelemetry() throws Exception {
        log.warn("deleteTelemetry...");
        long startMillis = System.currentTimeMillis();
        long loopNumber = 0;
        long documentCount = -1;
        long exceptionCount = -1;
        long initialDocumentCount = 0;
        boolean continueToProcess = true;

        while (continueToProcess) {
            loopNumber++;
            documentCount = epaOzoneTelemetryRepository.countAllDocuments();
            log.warn("loop number " + loopNumber + " document count: " + formattedCount(documentCount));
            if (loopNumber == 1) {
                initialDocumentCount = documentCount;
            }
            if (documentCount > 0) {
                try {
                    epaOzoneTelemetryRepository.deleteAll();
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
        documentCount = epaOzoneTelemetryRepository.countAllDocuments();
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
