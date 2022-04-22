package org.interviews.datadog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AlertManagerImpl implements AlertManager {
    private static final Logger LOG = LogManager.getLogger(AlertManagerImpl.class);

    @Override
    public void clearAlert() {
        LOG.warn("***** Alert cleared! ***** ");
    }

    @Override
    public void triggerAlert() {
        // In real-life this would do something more useful
        LOG.warn("***** Alert triggered! *****");
    }
}
