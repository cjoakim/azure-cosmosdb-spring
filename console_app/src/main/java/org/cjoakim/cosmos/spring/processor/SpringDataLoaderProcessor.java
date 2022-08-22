package org.cjoakim.cosmos.spring.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.model.*;
import org.cjoakim.cosmos.spring.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

/**
 * An instance of this class is created and executed from the DataCommandLineApp
 * main class to load CosmosDB from the previously transformed data.
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Component
@Data
@NoArgsConstructor
@Slf4j
public class SpringDataLoaderProcessor extends ConsoleAppProcessor implements AppConstants {

    private long   skipCount  = 0;
    private long   outputDocCount = 0;
    private long   maxRecords = Long.MAX_VALUE;
    private String infile;
    private String loadType;

    @Autowired
    private EpaOzoneTelemetryRepository epaOzoneTelemetryRepository = null;

    private boolean doWrites = false;

    public void process() throws Exception {

        doWrites = AppConfiguration.booleanArg(DO_WRITES_FLAG);

        log.warn("process, skipCount:  " + skipCount);
        log.warn("process, maxRecords: " + maxRecords);
        log.warn("process, loadType:   " + loadType);
        log.warn("process, infile:     " + infile);
        log.warn("process, doWrites:   " + doWrites);

        if (loadType.equalsIgnoreCase("epa_ozone_telemetry")) {
            loadEpaOzoneTelemetry();
        }
    }

    private void loadEpaOzoneTelemetry() throws Exception {
        log.warn("loadEpaOzoneTelemetry...");

        try {
            long lineNumber = 0;
            long linesProcessed = 0;
            ObjectMapper mapper = new ObjectMapper();

            Path path = Paths.get(infile);
            BufferedReader reader = Files.newBufferedReader(path);
            String line = null;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (lineNumber >= skipCount) {
                    if (linesProcessed < maxRecords) {
                        linesProcessed++;
                        log.warn(line);
                        EpaOzoneTelemetryEvent event = mapper.readValue(line.trim(), EpaOzoneTelemetryEvent.class);
                        event.setId(UUID.randomUUID().toString());
                        log.warn("EVENT: " + event.asJson(true));
                        Object result = epaOzoneTelemetryRepository.save(event);
                        log.warn("RESULT: " + result);
                    }
                }
            }
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
//        try {
//            ObjectMapper objectMapper = new ObjectMapper();
//            ObjectReader objectReader = objectMapper.reader().forType(new TypeReference<List<EpaOzoneTelemetryEvent>>(){});
//            List<EpaOzoneTelemetryEvent> entities = objectReader.readValue(Paths.get(infile).toFile());
//            log.warn("infile read: " + infile + ", count: " + entities.size());
//
//            for (int i = 0; i < entities.size(); i++) {
//                EpaOzoneTelemetryEvent entity = entities.get(i);
//                log.warn("entity: " + entity.asJson(true));
//                if (doWrites) {
//                    try {
//                        //Object result = epaOzoneTelemetryRepository.save(null);
//                    }
//                    catch (Exception e) {
//                        log.error("error loading document: " + entity.asJson(true));
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
