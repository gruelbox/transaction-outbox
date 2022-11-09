package com.synaos.transactionoutbox.acceptance;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class RemoteCallService {
    private boolean called;

    private boolean blocked;

    public void callRemote(boolean throwException) {
        if (throwException) {
            throw new RuntimeException("Thrown on purpose");
        }
        called = true;
    }

    public boolean isCalled() {
        return called;
    }

    public void setCalled(boolean called) {
        this.called = called;
    }

    public void block() {
        this.blocked = true;
    }

    public boolean isBlocked() {
        return blocked;
    }

    public void setBlocked(boolean blocked) {
        this.blocked = blocked;
    }
}
