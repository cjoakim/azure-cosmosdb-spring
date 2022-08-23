package org.cjoakim.cosmos.spring.processor;

import com.azure.spring.data.cosmos.core.CosmosTemplate;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.io.FileUtil;
import org.cjoakim.cosmos.spring.model.Library;
import org.cjoakim.cosmos.spring.model.Triple;
import org.cjoakim.cosmos.spring.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Component
@Slf4j
public class RepoQueryProcessor extends ConsoleAppProcessor implements AppConstants {

    private EpaOzoneTelemetryRepository epaOzoneTelemetryRepository = null;

    private CosmosTemplate template;

    @Autowired
    public RepoQueryProcessor(
            EpaOzoneTelemetryRepository epaOzoneTelemRepo,
            CosmosTemplate t) {
        super();
        this.epaOzoneTelemetryRepository = epaOzoneTelemRepo;
        this.template = t;
        log.warn("RepoQueryProcessor autowired constructor called");
    }

    public void process() throws Exception {

        FileUtil fu = new FileUtil();

        if (true) {
            log.warn("---");
            log.warn("process template count");

            // example of using CosmosTemplate outside of a Repository
            log.warn("template: " + template);
            long count = template.count("telemetry");
            log.warn("doc count in telemetry container: " + count);
            log.warn("last_request_charge: " + ResponseDiagnosticsProcessorImpl.getLastRequestCharge());
        }

        log.warn("countAllDocuments: " + epaOzoneTelemetryRepository.countAllDocuments());
    }
}
