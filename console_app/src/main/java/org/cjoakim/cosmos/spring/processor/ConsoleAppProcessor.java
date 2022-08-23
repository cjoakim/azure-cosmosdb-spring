package org.cjoakim.cosmos.spring.processor;

import java.text.DecimalFormat;

/**
 * Interface for "processor" classes created and invoked from the DataCommandLineApp
 * main class, a "console app"
 *
 * Chris Joakim, Microsoft, August 2022
 */

public abstract class ConsoleAppProcessor {

    public abstract void process() throws Exception;

    protected String formattedCount(long value) {
        DecimalFormat df = new DecimalFormat("###,###,###,###");
        return df.format(value);
    }
}
