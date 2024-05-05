package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import com.ericsson.bss.cassandra.ecchronos.core.utils.StatusLogger;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;

public class MetricInspector {

    private static final Logger LOG = LoggerFactory.getLogger(MetricInspector.class);
    private final MeterRegistry myMeterRegistry;

    private long repair_failure_count_since_last_report= 0;

    private long total_recorded_failures = 0;

    private int myrepairFailureThreshold = 0;

    private int myRepairFailureTimeWindow = 30;

    private LocalDateTime recordingStartTimestamp = LocalDateTime.now();


    public MetricInspector(MeterRegistry myMeterRegistry, int repairFailureThreshold, int repairFailureTimeWindow) {
        this.myMeterRegistry = myMeterRegistry;
        this.myrepairFailureThreshold = repairFailureThreshold;
        this.myRepairFailureTimeWindow = repairFailureTimeWindow;
    }



    /**
     * @return
     */

    public void startInspection() {
        java.util.Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                insepectMeterRegistryForRepairFailures();
            }
        }, 0, myRepairFailureTimeWindow * 1000);
    }


        private void insepectMeterRegistryForRepairFailures() {

            io.micrometer.core.instrument.Timer nodeRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                    .tags("successful", "false")
                    .timer();
            if(nodeRepairSessions != null){
                total_recorded_failures = nodeRepairSessions.count();
            }

            if(total_recorded_failures - repair_failure_count_since_last_report > myrepairFailureThreshold)
            {
                //reset count failure and reinitialize time window
                repair_failure_count_since_last_report = total_recorded_failures;
                recordingStartTimestamp = LocalDateTime.now();
                StatusLogger.log(myMeterRegistry);

            }

            resetRepairFailureCount();


    }

    /**
     * If in defined time window, number of repair failure has not crossed the configured number, then
     * reset failure count for new timed window.Also reinitialize timewindow
     */
        private void resetRepairFailureCount() {

            LocalDateTime currentTimeStamp = LocalDateTime.now();
            LocalDateTime thirtyMinutesAgo = currentTimeStamp.minus(30, ChronoUnit.MINUTES);

            if(recordingStartTimestamp.isBefore(thirtyMinutesAgo)){
                repair_failure_count_since_last_report = total_recorded_failures;
                recordingStartTimestamp = LocalDateTime.now();
            }


        }


}
