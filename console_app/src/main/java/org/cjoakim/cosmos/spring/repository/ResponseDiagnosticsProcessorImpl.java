package org.cjoakim.cosmos.spring.repository;

import com.azure.spring.data.cosmos.core.ResponseDiagnostics;
import com.azure.spring.data.cosmos.core.ResponseDiagnosticsProcessor;
import lombok.extern.slf4j.Slf4j;
import org.cjoakim.cosmos.spring.AppConfiguration;
import org.cjoakim.cosmos.spring.AppConstants;
import org.springframework.lang.Nullable;

/**
 *
 * Chris Joakim, Microsoft, September 2022
 */

@Slf4j
public class ResponseDiagnosticsProcessorImpl implements ResponseDiagnosticsProcessor, AppConstants {

    public static ResponseDiagnostics lastResponseDiagnostics;
    private boolean verbose;

    public ResponseDiagnosticsProcessorImpl() {
        super();
        verbose = AppConfiguration.booleanArg(VERBOSE_FLAG);
        log.warn("constructor, verbose: " + verbose);
    }

    @Override
    public void processResponseDiagnostics(@Nullable ResponseDiagnostics responseDiagnostics) {

        lastResponseDiagnostics = responseDiagnostics;
        if (responseDiagnostics != null) {
            if (verbose) {
                log.debug("ResponseDiagnostics: " + responseDiagnostics);
            }
        }
    }

    public static double getLastRequestCharge() {

        if (lastResponseDiagnostics != null) {
            try {
                return lastResponseDiagnostics.getCosmosResponseStatistics().getRequestCharge();
            }
            catch (Exception e) {
                // ignore for now
            }
        }
        return -1.0; // value indicating lack of success (less than zero)
    }

}
