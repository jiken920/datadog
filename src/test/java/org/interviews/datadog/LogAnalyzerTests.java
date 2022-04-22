package org.interviews.datadog;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * Unit test for LogAnalyzer
 */
@RunWith(MockitoJUnitRunner.class)
public class LogAnalyzerTests
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void testLogAnalyzerAlerts() throws IOException {
        AlertManager mockAlertManager = mock(AlertManagerImpl.class);
        LogAnalyzer logAnalyzer = new LogAnalyzer(10, 120, 10, mockAlertManager);
        logAnalyzer.analyze("src/test/resources/sample_csv.txt");

        // We should trigger and clear the alert twice with this test file.
        Mockito.verify(mockAlertManager, Mockito.times(2)).triggerAlert();
        Mockito.verify(mockAlertManager, Mockito.times(2)).clearAlert();
    }
}
