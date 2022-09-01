package org.cjoakim.cosmos.spring.processor;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.dao.CosmosSdkDao;
import org.cjoakim.cosmos.spring.io.FileUtil;
import org.cjoakim.cosmos.spring.model.TelemetryEvent;
import org.cjoakim.cosmos.spring.model.TelemetryQueryResults;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

/**
 *
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Component
@Data
@NoArgsConstructor
@Slf4j
public class SdkDaoQueryProcessor extends ConsoleAppProcessor implements AppConstants {
    @Value("${spring.cloud.azure.cosmos.endpoint}")
    public String uri;
    @Value("${spring.cloud.azure.cosmos.key}")
    public String key;
    @Value("${spring.cloud.azure.cosmos.database}")
    private String dbName;
    private String container;
    private boolean verbose;
    private FileUtil fileUtil = new FileUtil();
    private CosmosSdkDao dao = new CosmosSdkDao();

    public void process() throws Exception {

        try {
            verbose  = AppConfiguration.booleanArg(VERBOSE_FLAG);
            dao.initialize(uri, key, dbName, verbose);
            dao.setCurrentContainer(container);

            TelemetryQueryResults resultsStruct = dao.getAllTelemetry();
            log.warn("all_telemetry docs count: " + resultsStruct.getDocumentCount());
            saveResults(resultsStruct, "sdkTelemetry");

        }
        finally {
            dao.close();
        }
    }

    private void saveResults(TelemetryQueryResults resultsStruct, String queryName) {

        String outfile = "tmp/" + queryName + ".json";
        try {
            fileUtil.writeJson(resultsStruct, outfile, true, true);
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
