package org.cjoakim.cosmos.spring.model;

import com.azure.spring.data.cosmos.core.mapping.Container;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.cjoakim.cosmos.spring.AppConstants;

/**
 *
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Container(containerName=AppConstants.TELEMETRY_CONTAINER_NAME)
public class TelemetryEvent implements AppConstants {

    private String id;  // CosmosDB document ID
    private String pk;  // CosmosDB partition key

    private String stateCode;
    private String countyCode;
    private String siteNum;
    private double latitude;
    private double longitude;
    private String datum;
    private String metric;
    private String localDateTime;
    private String gmtDateTime;
    private String uom;
    private int    observationCount;
    private int    nullObservations;
    private double meanObservation;

    private String _etag;  // CosmosDB document version hash for OCC
    private long   _ts;    // CosmosDB document timestamp

    public String asJson(boolean pretty) throws Exception {

        try {
            ObjectMapper mapper = new ObjectMapper();
            if (pretty) {
                return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
            }
            else {
                return mapper.writeValueAsString(this);
            }
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}

// The given CSVRecord has these fields:
// field 0 : State Code
// field 1 : County Code
// field 2 : Site Num
// field 3 : Parameter Code
// field 4 : POC
// field 5 : Latitude
// field 6 : Longitude
// field 7 : Datum
// field 8 : Parameter Name
// field 9 : Date Local
// field 10 : Time Local
// field 11 : Date GMT
// field 12 : Time GMT
// field 13 : Sample Duration
// field 14 : Pollutant Standard
// field 15 : Units of Measure
// field 16 : Observation Count
// field 17 : Observations with Events
// field 18 : Null Observations
// field 19 : Mean Including All Data
// field 20 : Mean Excluding All Flagged Data
// field 21 : Mean Excluding Concurred Flags
// field 22 : Date of Last Change
