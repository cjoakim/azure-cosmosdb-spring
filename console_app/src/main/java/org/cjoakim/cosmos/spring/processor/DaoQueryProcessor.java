package org.cjoakim.cosmos.spring.processor;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.dao.CosmosDAO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 *
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Component
@NoArgsConstructor
@Slf4j
public class DaoQueryProcessor extends ConsoleAppProcessor implements AppConstants {

    @Value("${spring.cloud.azure.cosmos.endpoint}")
    public String uri;

    //@Value("${azure.cosmos.key}")
    @Value("${spring.cloud.azure.cosmos.key}")
    public String key;

    @Value("${spring.cloud.azure.cosmos.database}")
    private String dbName;

    private CosmosDAO dao = new CosmosDAO();

    public void process() throws Exception {

        try {
            String tenant = AppConfiguration.getTenant();
            log.warn("process, tenant: " + tenant);
            log.warn("process, uri:    " + uri);

            dao.initialize(uri, key, dbName);

            StringBuffer sb = new StringBuffer();
            sb.append("select * from c where c.doctype = 'triple'");

        }
        finally {
            dao.close();
        }
    }
}
