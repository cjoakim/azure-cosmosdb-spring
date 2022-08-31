package org.cjoakim.cosmos.spring.repository;

import org.cjoakim.cosmos.spring.model.TelemetryEvent;

import java.util.ArrayList;

/**
 * This interface was created to extend the TelemetryRepository, which in turn extends
 * CosmosRepository<TelemetryEvent, String> from the CosmosDB Spring Data SDK.
 *
 * This demonstrates how to leverage more of the power of the CosmosDB SQL syntax, by using
 * "Criteria" objects and an Autowired "CosmosTemplate" object.
 *
 * See class TelemetryRepositoryExtensionsImpl in this package, which implements this interface.
 *
 * Chris Joakim, Microsoft, September 2022
 */
public interface TelemetryRepositoryExtensions {

    public Iterable<TelemetryEvent> findByStateCodeAndSiteNumbers(String stateCode, ArrayList<String> siteNumbers);
}
