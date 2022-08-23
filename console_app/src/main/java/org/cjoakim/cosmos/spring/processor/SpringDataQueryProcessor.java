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
    @Autowired
    private EpaOzoneTelemetryRepository telemetryRepository = null;

    public void process() throws Exception {

        verbose  = AppConfiguration.booleanArg(VERBOSE_FLAG);
        long count = -1;

        log.warn("process, queryTypes:  " + queryTypes);

        if (queryTypes.contains("count")) {
            count = telemetryRepository.count();
            log.warn("telemetery repository, count: " + count);
        }
    }
}
