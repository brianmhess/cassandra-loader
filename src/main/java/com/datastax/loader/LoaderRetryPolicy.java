package com.datastax.loader;

import java.lang.String;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;

import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;

class LoaderRetryPolicy implements RetryPolicy {
    private int numRetries;

    public LoaderRetryPolicy(int inNumRetries) {
	numRetries = inNumRetries;
    }

    // Taken from DefaultRetryPolicy
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl,
				       int requiredResponses, 
				       int receivedResponses, 
				       boolean dataRetrieved, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        return receivedResponses >= requiredResponses && !dataRetrieved 
	    ? RetryDecision.retry(cl) 
	    : RetryDecision.rethrow();
    }

    // Taken from DefaultRetryPolicy
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl,
				       int requiredReplica, int aliveReplica, 
				       int nbRetry) {
        return RetryDecision.rethrow();
    }

    public RetryDecision onWriteTimeout(Statement statement, 
					ConsistencyLevel cl, 
					WriteType writeType, int requiredAcks, 
					int receivedAcks, int nbRetry) {
        if (nbRetry >= numRetries)
            return RetryDecision.rethrow();

        return RetryDecision.retry(cl);
    }
}
