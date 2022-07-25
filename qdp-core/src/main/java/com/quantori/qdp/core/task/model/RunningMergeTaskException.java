package com.quantori.qdp.core.task.model;

public class RunningMergeTaskException extends RuntimeException {

    public RunningMergeTaskException(String index) {
        super(message(index));
    }

    private static String message(String index) {
        return "Editing of index " + index + " is not allowed during merge process.";
    }
}
