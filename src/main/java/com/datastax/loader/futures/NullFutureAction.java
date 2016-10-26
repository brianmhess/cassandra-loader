package com.datastax.loader.futures;

import com.datastax.driver.core.ResultSet;

public class NullFutureAction implements FutureAction {
    public void onSuccess(ResultSet rs, String line) { }
    public void onFailure(Throwable t, String line) { }
    public void onTooManyFailures() { }
}
