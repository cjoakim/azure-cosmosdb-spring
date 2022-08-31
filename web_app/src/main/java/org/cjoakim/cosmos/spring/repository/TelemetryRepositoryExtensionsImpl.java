package org.cjoakim.cosmos.spring.repository;

import com.azure.spring.data.cosmos.core.CosmosTemplate;
import com.azure.spring.data.cosmos.core.query.CosmosQuery;
import com.azure.spring.data.cosmos.core.query.Criteria;
import com.azure.spring.data.cosmos.core.query.CriteriaType;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.model.TelemetryEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.query.parser.Part;

import java.util.ArrayList;
import java.util.Collections;

/**
 * This class implements the TelemetryRepositoryExtensions interface and demonstrates how to leverage
 * more of the power of the CosmosDB SQL syntax, by using "Criteria" objects and an Autowired "CosmosTemplate"
 * object.
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Slf4j
public class TelemetryRepositoryExtensionsImpl implements TelemetryRepositoryExtensions {
    private CosmosTemplate template;

    @Autowired
    public TelemetryRepositoryExtensionsImpl(CosmosTemplate t) {
        super();
        this.template = t;
        log.warn("TelemetryRepositoryExtensionsImpl constructor, template: " + this.template);
    }

    public Iterable<TelemetryEvent> findByStateCodeAndSiteNumbers(String stateCode, ArrayList<String> siteNumbers) {

        String containerName = AppConstants.TELEMETRY_CONTAINER_NAME;

        Criteria criteria1 = Criteria.getInstance(
                CriteriaType.IS_EQUAL, "stateCode", Collections.singletonList(stateCode),
                Part.IgnoreCaseType.NEVER);

        Criteria criteria2 = Criteria.getInstance(
                CriteriaType.IN, "siteNum", Collections.singletonList(siteNumbers),
                Part.IgnoreCaseType.NEVER);

        Criteria allCriteria = Criteria.getInstance(CriteriaType.AND, criteria1, criteria2);

        CosmosQuery query = new CosmosQuery(allCriteria);

        return template.find(query, TelemetryEvent.class, containerName);
    }
}
