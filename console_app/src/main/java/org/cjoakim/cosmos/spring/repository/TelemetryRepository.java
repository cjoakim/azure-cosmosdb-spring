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

//        {
//        "id": "018d34ba-52c2-4edf-931a-3d22b2b937e4",
//        "pk": "33.904039|-86.053867",
//        "stateCode": "01",
//        "countyCode": "055",
//        "siteNum": "0011",
//        "latitude": 33.904039,
//        "longitude": -86.053867,
//        "datum": "NAD83",
//        "metric": "Ozone",
//        "localDateTime": "2021-03-01 19:00",
//        "gmtDateTime": "2021-03-02 01:00",
//        "uom": "Parts per million",
//        "observationCount": 7,
//        "nullObservations": 1,
//        "meanObservation": 0.03,
//        "_rid": "gklzAKAAj3sOAAAAAAAAAA==",
//        "_self": "dbs/gklzAA==/colls/gklzAKAAj3s=/docs/gklzAKAAj3sOAAAAAAAAAA==/",
//        "_etag": "\"4c01dde8-0000-0100-0000-6304de220000\"",
//        "_attachments": "attachments/",
//        "_ts": 1661263394
//        }
