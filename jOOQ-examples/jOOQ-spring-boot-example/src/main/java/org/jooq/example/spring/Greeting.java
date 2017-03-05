package org.jooq.example.spring;

/**
 * Authored by clkim on 3/5/17.
 */

public class Greeting {
    private String country;
    private String device;
    private String tester;

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getTester() {
        return tester;
    }

    public void setTester(String tester) {
        this.tester = tester;
    }
}
