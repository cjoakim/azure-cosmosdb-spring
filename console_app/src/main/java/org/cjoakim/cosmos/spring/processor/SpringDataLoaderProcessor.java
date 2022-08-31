package org.cjoakim.cosmos.spring.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.model.TelemetryEvent;
import org.cjoakim.cosmos.spring.repository.TelemetryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * This ConsoleAppProcessor is used to load the EPA Ozone Telemetry data into CosmosDB
 * with the Spring Data SDK.
 *
 * Chris Joakim, Microsoft, September 2022
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
    private TelemetryRepository telemetryRepository = null;

    private boolean doWrites = false;
    private boolean verbose  = false;

    public void process() throws Exception {

        doWrites = AppConfiguration.booleanArg(DO_WRITES_FLAG);
        verbose  = AppConfiguration.booleanArg(VERBOSE_FLAG);

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
        BufferedReader reader = null;
        long startMillis = System.currentTimeMillis();
        long lineNumber = 0;
        long linesProcessed = 0;
        long malformedJsonLines = 0;
        long documentsWritten = 0;
        long documentsProcessed = 0;
        ObjectMapper mapper = new ObjectMapper();

        try {
            Path path = Paths.get(infile);
            reader = Files.newBufferedReader(path);
            String line = null;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (lineNumber >= skipCount) {
                    if (linesProcessed < maxRecords) {
                        linesProcessed++;
                        TelemetryEvent event = null;
                        try {
                            event = mapper.readValue(line.trim(), TelemetryEvent.class);
                            event.setId(UUID.randomUUID().toString());
                            if (doWrites) {
                                Object result = telemetryRepository.save(event);
                                documentsWritten++;
                                if (verbose || (documentsWritten % 1000) == 0) {
                                    log.warn("document number " + documentsWritten + " result: " + result + "  " + result.getClass().getName());
                                }
                            }
                            else {
                                documentsProcessed++;
                                if (verbose || (documentsProcessed % 1000) == 0) {
                                    log.warn("document number " + documentsProcessed + ", event: " + event.asJson(true));
                                }
                            }
                        }
                        catch (Throwable t) {
                            malformedJsonLines++;
                            log.error("exception on line " + lineNumber + ": " + line);
                        }
                    }
                    else {
                        break; // terminate the read while-loop
                    }
                }
            }
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
        finally {
            if (reader != null) {
                log.warn("reader on infile " + infile);
                reader.close();
                log.warn("reader closed");
            }
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            double elapsedMinutes = (double) elapsedMillis / 60000.0;
            log.warn("skip count:           " + skipCount);
            log.warn("max records:          " + maxRecords);
            log.warn("lines read:           " + formattedCount(lineNumber));
            log.warn("documents written:    " + formattedCount(documentsWritten));
            log.warn("documents processed:  " + formattedCount(documentsProcessed));
            log.warn("malformed json lines: " + malformedJsonLines);
            log.warn("elapsed ms:           " + elapsedMillis);
            log.warn("elapsed minutes:      " + elapsedMinutes);
            if (doWrites) {
                log.warn("docs per minute:      " + ((double) documentsWritten) / elapsedMinutes);
            }
            else {
                log.warn("docs per minute:      " + ((double) documentsProcessed) / elapsedMinutes);
            }
        }
    }
}

//10:27:11.407 [cosmos-parallel-2] INFO  c.a.c.i.RxDocumentClientImpl - Getting database account endpoint from https://cjoakimcosmossql.documents.azure.com:443/
//10:27:53.145 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - document number 46000 result: TelemetryEvent(id=38cd8b3a-87f3-434e-aee3-50f1331ee14b, pk=34.687761|-86.586362, stateCode=01, countyCode=089, siteNum=0014, latitude=34.687761, longitude=-86.586362, datum=WGS84, metric=Ozone, localDateTime=2021-06-07 07:00, gmtDateTime=2021-06-07 13:00, uom=Parts per million, observationCount=8, nullObservations=0, meanObservation=0.023, _etag="7300e63c-0000-0100-0000-6304e3e90000", _ts=1661264873)  org.cjoakim.cosmos.spring.model.TelemetryEvent
//10:28:38.715 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - document number 47000 result: TelemetryEvent(id=70f3b9cc-e71b-4cbd-ada7-7b152367bbd1, pk=34.687761|-86.586362, stateCode=01, countyCode=089, siteNum=0014, latitude=34.687761, longitude=-86.586362, datum=WGS84, metric=Ozone, localDateTime=2021-08-05 08:00, gmtDateTime=2021-08-05 14:00, uom=Parts per million, observationCount=8, nullObservations=0, meanObservation=0.048, _etag="73002748-0000-0100-0000-6304e4160000", _ts=1661264918)  org.cjoakim.cosmos.spring.model.TelemetryEvent
//10:29:23.937 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - document number 48000 result: TelemetryEvent(id=6075f6f0-5362-4b69-9c00-113563d4a0e4, pk=34.687761|-86.586362, stateCode=01, countyCode=089, siteNum=0014, latitude=34.687761, longitude=-86.586362, datum=WGS84, metric=Ozone, localDateTime=2021-10-03 07:00, gmtDateTime=2021-10-03 13:00, uom=Parts per million, observationCount=8, nullObservations=0, meanObservation=0.025, _etag="73007353-0000-0100-0000-6304e4430000", _ts=1661264963)  org.cjoakim.cosmos.spring.model.TelemetryEvent
//10:30:09.129 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - document number 49000 result: TelemetryEvent(id=11cda0af-4ca1-4476-86bc-b1e7d670239b, pk=34.772727|-86.756174, stateCode=01, countyCode=089, siteNum=0022, latitude=34.772727, longitude=-86.756174, datum=WGS84, metric=Ozone, localDateTime=2021-04-03 18:00, gmtDateTime=2021-04-04 00:00, uom=Parts per million, observationCount=8, nullObservations=0, meanObservation=0.018, _etag="7300d05e-0000-0100-0000-6304e4710000", _ts=1661265009)  org.cjoakim.cosmos.spring.model.TelemetryEvent
//10:30:54.302 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - document number 50000 result: TelemetryEvent(id=3a4ef644-5f84-47a0-99ef-2ac538bfabab, pk=34.772727|-86.756174, stateCode=01, countyCode=089, siteNum=0022, latitude=34.772727, longitude=-86.756174, datum=WGS84, metric=Ozone, localDateTime=2021-06-01 15:00, gmtDateTime=2021-06-01 21:00, uom=Parts per million, observationCount=8, nullObservations=0, meanObservation=0.04, _etag="7300376a-0000-0100-0000-6304e49e0000", _ts=1661265054)  org.cjoakim.cosmos.spring.model.TelemetryEvent
//10:30:54.304 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - reader on infile data/epa/8hour_44201_2021/ozone_telemetry.json
//10:30:54.306 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - reader closed
//10:30:54.306 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - skip count:           0
//10:30:54.306 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - max records:          50000
//10:30:54.308 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - lines read:           50,000
//10:30:54.309 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - documents written:    50,000
//10:30:54.310 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - documents processed:  0
//10:30:54.311 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - malformed json lines: 0
//10:30:54.311 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - elapsed ms:           2324820
//10:30:54.313 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - elapsed minutes:      38.747
//10:30:54.314 [restartedMain] WARN  o.c.c.s.p.SpringDataLoaderProcessor - docs per minute:  1290.4224843213667
//10:30:54.314 [restartedMain] WARN  org.cjoakim.cosmos.spring.App - spring app exiting
