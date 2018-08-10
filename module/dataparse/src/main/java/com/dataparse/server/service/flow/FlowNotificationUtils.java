package com.dataparse.server.service.flow;

import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.DecimalFormat;
import java.util.*;

public class FlowNotificationUtils {

    private static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###,###");
    private static FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("mm'm'ss's'", TimeZone.getTimeZone("UTC"));

    public static String getProcessionTotalString(Long successItemsCount, Long time, Integer tasksCount){

        return String.format("%s row%s %s uploaded and ingested in %s. ",
                DECIMAL_FORMAT.format(successItemsCount),
                successItemsCount > 1 ? "s" : "",
                tasksCount > 1 ? "across " + tasksCount + " tabs": "",
                DATE_FORMAT.format(time)
        );
    }

    public static String getProcessionTotalString(TaskResult taskResult) {
        if(taskResult != null && taskResult instanceof IngestTaskResult) {
            IngestTaskResult task = (IngestTaskResult) taskResult;

            return getProcessionTotalString(task.getSuccessItemsCount(), task.getExecutionTime(), 1);
        }
        return null;
    }

    public static String getProcessionTotalString(TaskResult taskResult, Long executionTime) {
        if(taskResult != null && taskResult instanceof IngestTaskResult) {
            IngestTaskResult task = (IngestTaskResult) taskResult;

            return getProcessionTotalString(task.getSuccessItemsCount(), executionTime, 1);
        }
        return null;
    }

    private static String getProcessionErrorsString(FlowSettings flowSettings, Long errorsCount, List<ErrorValue> errors){
        String result = "";
        String message = "%d row%s had a value that could not be parsed. Based on your import settings, we ";
        String replaceMessage = "set %s value to null. ";
        String skipMessage = "skipped %s. ";

        if(errorsCount > 0){
            boolean replace = flowSettings.getIngestErrorMode().equals(IngestErrorMode.REPLACE_WITH_NULL);
            boolean pluralize = errorsCount > 1;
            String s = pluralize ? "s" : "";
            result += String.format(message, errorsCount, s);
            result += replace ? String.format(replaceMessage, pluralize ? "it's" : "them") : String.format(skipMessage, pluralize ? "them" : "it");
            result += " Example error: " + errors.get(0).getDescription();
        }
        return result;
    }

    public static String getErrorMessageByField(TaskResult taskResult, FlowSettings flowSettings) {
        if(taskResult instanceof IngestTaskResult){
            IngestTaskResult syncResult = (IngestTaskResult) taskResult;
            return getProcessionErrorsString(flowSettings, syncResult.getProcessionErrorsCount(), syncResult.getProcessionErrors());
        } else {
            throw new RuntimeException("Unknown task result class: " + taskResult.getClass().getSimpleName());
        }
    }
}
