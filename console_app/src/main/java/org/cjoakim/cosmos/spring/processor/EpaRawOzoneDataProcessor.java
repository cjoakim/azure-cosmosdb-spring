package org.cjoakim.cosmos.spring.processor;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.cjoakim.cosmos.spring.AppConstants;
import org.cjoakim.cosmos.spring.model.EpaOzoneTelemetryEvent;

import java.io.*;
import java.text.DecimalFormat;

/**
 *
 * Chris Joakim, Microsoft, August 2022
 */

@Slf4j
@Data
public class EpaRawOzoneDataProcessor extends ConsoleAppProcessor implements AppConstants {

    private long skipCount  = 0;
    private long outputDocCount = 0;
    private long maxRecords = Long.MAX_VALUE;

    private String partitionKeyStrategy;

    public void process() throws Exception {

        log.warn("skipCount:  " + skipCount);
        log.warn("maxRecords: " + maxRecords);
        //log.warn("Long.MAX_VALUE: " + Long.MAX_VALUE);  // 9223372036854775807

        BufferedWriter writer = null;
        String infileName  = "data/epa/8hour_44201_2021/8hour_44201_2021.csv";
        String outfileName = "data/epa/8hour_44201_2021/ozone_telemetry.json";
        long startMillis = System.currentTimeMillis();

        try {
            Reader in = new FileReader(infileName);
            Iterable<CSVRecord> recordsIterator = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            long recordNum = 0;

            File outFile = new File(outfileName);
            FileOutputStream fos = new FileOutputStream(outFile);
            writer = new BufferedWriter(new OutputStreamWriter(fos));

            for (CSVRecord record : recordsIterator) {
                recordNum++;
                if (recordNum >= skipCount) {
                    if (outputDocCount < maxRecords) {
                        outputDocCount++;
                        if ((outputDocCount % 10000) == 0) {
                            log.warn("processing document number " + formattedCount(outputDocCount));
                        }
                        EpaOzoneTelemetryEvent event = recordToEvent(record, recordNum);
                        if (event != null) {
                            writer.write(event.asJson(false));
                            writer.newLine();
                        }
                    }
                    else {
                        log.warn("exiting read loop at maxRecords: " + maxRecords);
                        break;
                    }
                }
            }
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
        finally {
            if (writer != null) {
                log.warn("closing outfile " + outfileName);
                writer.close();
                log.warn("outfile closed");
            }
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            double elapsedMinutes = (double) elapsedMillis / 60000.0;
            log.warn("output doc count: " + formattedCount(outputDocCount));
            log.warn("elapsed minutes:  " + elapsedMinutes);
        }
    }

    private EpaOzoneTelemetryEvent recordToEvent(CSVRecord record, long recordNumber) {

        try {
            EpaOzoneTelemetryEvent event = new EpaOzoneTelemetryEvent();
            event.setStateCode(record.get(0));
            event.setCountyCode(record.get(1));
            event.setSiteNum(record.get(2));
            event.setLatitude(Double.parseDouble(record.get(5)));
            event.setLongitude(Double.parseDouble(record.get(6)));

            event.setDatum(record.get(7));
            event.setMetric(record.get(8));

            String localDate = record.get(9);
            String localTime = record.get(10);
            event.setLocalDateTime("" + localDate + " " + localTime);

            String gmtDate   = record.get(11);
            String gmtTime   = record.get(12);
            event.setGmtDateTime("" + gmtDate + " " + gmtTime);

            event.setUom(record.get(15));
            event.setObservationCount(Integer.parseInt(record.get(16)));
            event.setNullObservations(Integer.parseInt(record.get(18)));
            event.setMeanObservation(Double.parseDouble(record.get(19)));

            setPartitionKey(event);

            return event;
        }
        catch (Exception e) {
            log.error("Exception on recordNumber " + recordNumber + " : " + record);
            return null;
        }
    }

    private void setPartitionKey(EpaOzoneTelemetryEvent event) {

        if (event != null) {
            event.setPk(event.getGmtDateTime());  // 'gmtDateTime' is the default pk attribute

            if (partitionKeyStrategy != null) {
                if (partitionKeyStrategy.equalsIgnoreCase("localDateTime")) {
                    event.setPk(event.getLocalDateTime());
                }
                else if (partitionKeyStrategy.equalsIgnoreCase("siteNum")) {
                    event.setPk(event.getSiteNum());
                }
                else if (partitionKeyStrategy.equalsIgnoreCase("stateCounty")) {
                    event.setPk("" + event.getStateCode() + "|" + event.getCountyCode());
                }
                else if (partitionKeyStrategy.equalsIgnoreCase("latLng")) {
                    event.setPk("" + event.getLatitude() + "|" + event.getLongitude());
                }
            }
        }
    }

    private String formattedCount(long value) {
        DecimalFormat df = new DecimalFormat("###,###,###,###");
        return df.format(value);
    }

    private void printFieldsList() {
        // Get-Content data/epa/8hour_44201_2021/8hour_44201_2021.csv | select -first 1
        // Get-Content data/epa/8hour_44201_2021/8hour_44201_2021.csv | select -first 1001 > data/epa/8hour_44201_2021/8hour_44201_2021_mini.csv

        String header = "State Code,County Code,Site Num,Parameter Code,POC,Latitude,Longitude,Datum,Parameter Name,Date Local,Time Local,Date GMT,Time GMT,Sample Duration,Pollutant Standard,Units of Measure,Observation Count,Observations with Events,Null Observations,Mean Including All Data,Mean Excluding All Flagged Data,Mean Excluding Concurred Flags,Date of Last Change";
        String[] tokens = header.split(",");
        for (int i = 0; i < tokens.length; i++) {
            System.out.println("        // field " + i + " : " + tokens[i]);
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
