package org.cjoakim.cosmos.spring.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;

import java.io.FileReader;
import java.io.Reader;

@Slf4j
public class EpaOzoneDataProcessor implements ConsoleAppProcessor, AppConstants {

    public void process() throws Exception {

        // Get-Content data/epa/8hour_44201_2021/8hour_44201_2021.csv | select -first 2
        try {
            String infile = "data/epa/8hour_44201_2021/8hour_44201_2021.csv";
            Reader in = new FileReader(infile);
            Iterable<CSVRecord> recordsIterator = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            long recordNum = 0;

            for (CSVRecord record : recordsIterator) {
                recordNum++;
                log.warn("recordNum " + recordNum + ": " + record);
                if (recordNum > 1000) {
                    break;
                }
            }

            log.warn("end of for loop at recordNum " + recordNum + "");
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
        finally {
            log.warn("finally");
        }
    }
}
