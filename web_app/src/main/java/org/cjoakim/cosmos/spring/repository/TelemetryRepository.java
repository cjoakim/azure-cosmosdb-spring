package org.cjoakim.cosmos.spring.repository;

import com.azure.spring.data.cosmos.repository.CosmosRepository;
import com.azure.spring.data.cosmos.repository.Query;
import org.cjoakim.cosmos.spring.model.TelemetryEvent;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

/**
 * Spring Data Repository for class TelemetryEvent.
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Component
@Repository
public interface TelemetryRepository extends
        CosmosRepository<TelemetryEvent, String>,
        TelemetryRepositoryExtensions {

    Iterable<TelemetryEvent> findByStateCode(String stateCode);

    Iterable<TelemetryEvent> findByStateCodeAndCountyCode(String stateCode, String countyCode);

    @Query("select value count(1) from c")
    long countAllDocuments();

    @Query( "select c.id, c.pk, c.stateCode, c.countyCode, c.siteNum, c.observationCount, c.nullObservations " +
            "from  c " +
            "where c.observationCount >= @observationCount " +
            "and   c.nullObservations >= @nullObservations")
    Iterable<TelemetryEvent>  findByObservationCount(
            @Param("observationCount") long observationCount,
            @Param("nullObservations") long nullObservations);

}
