package org.cjoakim.cosmos.spring.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.io.FileUtil;
import org.cjoakim.cosmos.spring.model.EpaOzoneTelemetryEvent;
import org.cjoakim.cosmos.spring.repository.EpaOzoneTelemetryRepository;
import org.cjoakim.cosmos.spring.repository.ResponseDiagnosticsProcessorImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.UUID;

/**
 * This ConsoleAppProcessor executes various Spring Data SDK queries vs the telemetry
 * container and captures the output as JSON files.
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Component
@Data
@NoArgsConstructor
@Slf4j
public class SpringDataQueryProcessor extends ConsoleAppProcessor implements AppConstants {

    private String queryTypes;
    private boolean verbose  = false;
    private FileUtil fileUtil = new FileUtil();
    private ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private EpaOzoneTelemetryRepository telemetryRepository = null;

    public void process() throws Exception {

        verbose  = AppConfiguration.booleanArg(VERBOSE_FLAG);
        long count = -1;

        log.warn("process, queryTypes:  " + queryTypes);

        if (queryTypes.contains(" count ")) {
            count = telemetryRepository.count();
            log.warn("telemetery repository, count: " + count);
            log.warn("count - last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
        }
        if (queryTypes.contains(" findByStateCode ")) {
            Iterable<EpaOzoneTelemetryEvent> iterable =
                    telemetryRepository.findByStateCode("01");
            log.warn("findByStateCode - last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
            saveResults(iterable, "findByStateCode");
        }
        if (queryTypes.contains(" findByStateCodeAndCountyCode ")) {
            Iterable<EpaOzoneTelemetryEvent> iterable =
                    telemetryRepository.findByStateCodeAndCountyCode("01", "003");
            log.warn("findByStateCodeAndCountyCode - last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
            saveResults(iterable, "findByStateCodeAndCountyCode");
        }
        if (queryTypes.contains(" findByObservationCount ")) {
            Iterable<EpaOzoneTelemetryEvent> iterable =
                    telemetryRepository.findByObservationCount(1, 1);
            log.warn("findByObservationCount - last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
            saveResults(iterable, "findByObservationCount");
        }
    }

    private void saveResults(Iterable<EpaOzoneTelemetryEvent> iterable, String queryName) {

        String outfile = "tmp/" + queryName + ".json";
        try {
            ArrayList<EpaOzoneTelemetryEvent> events = new ArrayList<EpaOzoneTelemetryEvent>();
            iterable.forEach(doc -> { events.add(doc); });
            fileUtil.writeJson(events, outfile, true, true);
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }
}

//{
//    "id": "d91d5d82-5e24-4f33-a8b2-c855970e333f",
//    "pk": "30.497478|-87.880258",
//    "stateCode": "01",
//    "countyCode": "003",
//    "siteNum": "0010",
//    "latitude": 30.497478,
//    "longitude": -87.880258,
//    "datum": "NAD83",
//    "metric": "Ozone",
//    "localDateTime": "2021-03-01 08:00",
//    "gmtDateTime": "2021-03-01 14:00",
//    "uom": "Parts per million",
//    "observationCount": 8,
//    "nullObservations": 0,
//    "meanObservation": 0.022,
//    "_rid": "gklzAKAAj3uh3wAAAAAACA==",
//    "_self": "dbs/gklzAA==/colls/gklzAKAAj3s=/docs/gklzAKAAj3uh3wAAAAAACA==/",
//    "_etag": "\"7f000f8b-0000-0100-0000-630539cd0000\"",
//    "_attachments": "attachments/",
//    "_ts": 1661286861
//}