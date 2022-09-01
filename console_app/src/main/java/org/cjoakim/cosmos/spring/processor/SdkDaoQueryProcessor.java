package org.cjoakim.cosmos.spring.processor;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.dao.CosmosSdkDao;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 *
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Component
@NoArgsConstructor
@Slf4j
public class SdkDaoQueryProcessor extends ConsoleAppProcessor implements AppConstants {

    @Value("${spring.cloud.azure.cosmos.endpoint}")
    public String uri;

    //@Value("${azure.cosmos.key}")
    @Value("${spring.cloud.azure.cosmos.key}")
    public String key;

    @Value("${spring.cloud.azure.cosmos.database}")
    private String dbName;

    private boolean verbose;

    private CosmosSdkDao dao = new CosmosSdkDao();

    public void process() throws Exception {

        try {
            verbose  = AppConfiguration.booleanArg(VERBOSE_FLAG);
            dao.initialize(uri, key, dbName, verbose);

            // TODO

        }
        finally {
            dao.close();
        }
    }
}
