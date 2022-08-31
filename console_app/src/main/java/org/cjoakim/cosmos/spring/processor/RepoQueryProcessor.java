package org.cjoakim.cosmos.spring.processor;

import com.azure.spring.data.cosmos.core.CosmosTemplate;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.io.FileUtil;
import org.cjoakim.cosmos.spring.repository.ResponseDiagnosticsProcessorImpl;
import org.cjoakim.cosmos.spring.repository.TelemetryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Component
@Slf4j
public class RepoQueryProcessor extends ConsoleAppProcessor implements AppConstants {

    private TelemetryRepository epaOzoneTelemetryRepository = null;

    private CosmosTemplate template;

    @Autowired
    public RepoQueryProcessor(
            TelemetryRepository epaOzoneTelemRepo,
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
