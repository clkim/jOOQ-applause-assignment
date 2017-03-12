package org.jooq.example.spring;

import com.google.common.collect.Lists;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.example.db.h2.tables.records.BugsRecord;
import org.jooq.example.db.h2.tables.records.DevicesRecord;
import org.jooq.example.db.h2.tables.records.TesterDeviceRecord;
import org.jooq.example.db.h2.tables.records.TestersRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.jooq.example.db.h2.Tables.*;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.inline;
import static org.junit.Assert.assertEquals;

/**
 * Authored by clkim on 3/4/17.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = Application.class)
//@SpringApplicationConfiguration(classes = Application.class)
public class ApplauseDemoTest {
    ArrayList<String> expectedTestersAllCountriesAllDevices = Lists.newArrayList("Taybin Rutkin", "Lucas Lowry", "Sean Wellington",
            "Miguel Bautista", "Stanley Chen", "Mingquan Zheng", "Leonard Sutton", "Darshini Thiagarajan", "Michael Lubavin");

    @Autowired
    DSLContext create;

    @Before
    public void populateTables() throws Exception {
        create.truncate(BUGS).execute();
        create.truncate(TESTER_DEVICE).execute();
        create.deleteFrom(DEVICES).execute(); // truncate seems does not work here for H2 because of foreign keys
        create.deleteFrom(TESTERS).execute(); // truncate seems does not work here for H2 because of foreign keys

        create.loadInto(TESTERS)
                .loadCSV(new File("src/main/resources/applause/testers.csv"))
                .fields(TESTERS.TESTER_ID, TESTERS.FIRST_NAME, TESTERS.LAST_NAME, TESTERS.COUNTRY, TESTERS.LAST_LOGIN)
                .execute();
        Result<TestersRecord> testers = create.selectFrom(TESTERS).fetch();
        assertEquals(9, testers.size());

        create.loadInto(DEVICES)
                .loadCSV(new File("src/main/resources/applause/devices.csv"))
                .fields(DEVICES.DEVICE_ID, DEVICES.DESCRIPTION)
                .execute();
        Result<DevicesRecord> devices = create.selectFrom(DEVICES).fetch();
        assertEquals(10, devices.size());

        create.loadInto(TESTER_DEVICE)
                .loadCSV(new File("src/main/resources/applause/tester_device.csv"))
                .fields(TESTER_DEVICE.TESTER_ID, TESTER_DEVICE.DEVICE_ID)
                .execute();
        Result<TesterDeviceRecord> testersDevices = create.selectFrom(TESTER_DEVICE).fetch();
        assertEquals(36, testersDevices.size());


        create.loadInto(BUGS)
                .loadCSV(new File("src/main/resources/applause/bugs.csv"))
                .fields(BUGS.BUG_ID, BUGS.DEVICE_ID, BUGS.TESTER_ID)
                .execute();
        Result<BugsRecord> bugs = create.selectFrom(BUGS).fetch();
        assertEquals(1000, bugs.size());
    }

    @Test
    public void testMultipleCountriesDevices() throws Exception {
        Collection<String> selDevices = Lists.newArrayList("Droid DNA", "iPhone 4", "Galaxy S4");
        Collection<String> selCountries = Lists.newArrayList("GB", "US");

        // tester_device left outer join with bugs, count bug_id column in order to sort on it
        Result<?> result = privCreate(selDevices, selCountries);
        assertEquals(5, result.size());

        // marshall result
        List<String> testers = new ArrayList<>();
        result.forEach(r ->
            testers.add(String.join(" ", r.getValue(TESTERS.FIRST_NAME), r.getValue(TESTERS.LAST_NAME)))
        );

        assertEquals(5, testers.size());
        ArrayList<String> expected = Lists.newArrayList("Taybin Rutkin", "Darshini Thiagarajan", "Michael Lubavin", "Miguel Bautista", "Leonard Sutton");
        assertEquals(expected, testers);
    }

    private Result<?> privCreate(Collection<String> selDevices, Collection<String> selCountries) {
        // tester_device left outer join with bugs, count bug_id column in order to sort on it
        Result<?> result = create
                .select(count(BUGS.BUG_ID), TESTERS.TESTER_ID, TESTERS.FIRST_NAME, TESTERS.LAST_NAME)
                .from(TESTER_DEVICE)
                .join(DEVICES).on(DEVICES.DESCRIPTION.in(selDevices))
                .join(TESTERS).on(TESTERS.COUNTRY.in(selCountries))
                .leftOuterJoin(BUGS).on(TESTERS.TESTER_ID.equal(BUGS.TESTER_ID).and(DEVICES.DEVICE_ID.equal(BUGS.DEVICE_ID)))
                .where(TESTER_DEVICE.TESTER_ID.equal(TESTERS.TESTER_ID).and(TESTER_DEVICE.DEVICE_ID.equal(DEVICES.DEVICE_ID)))
                .groupBy(TESTERS.TESTER_ID)
                .orderBy(inline(1).desc())
                .fetch();
        System.out.println(result);
        return result;
    }

    @Test
    public void testAllCountriesMultipleDevices() throws Exception {
        Result<?> allCountries = create
                .selectDistinct(TESTERS.COUNTRY)
                .from(TESTERS)
                .fetch();
        assertEquals(3, allCountries.size());

        Collection<String> selCountries = new ArrayList<>();
        allCountries.forEach(r -> selCountries.add(r.getValue(TESTERS.COUNTRY)));

        Collection<String> selDevices = Lists.newArrayList("Droid DNA", "iPhone 4", "Galaxy S4");

        Result<?> result = privCreate(selDevices, selCountries);
        // marshall result
        List<String> testers = new ArrayList<>();
        result.forEach(r ->
            testers.add(String.join(" ", r.getValue(TESTERS.FIRST_NAME), r.getValue(TESTERS.LAST_NAME)))
        );

        assertEquals(8, testers.size());
        ArrayList<String> expected = Lists.newArrayList("Taybin Rutkin", "Darshini Thiagarajan", "Lucas Lowry", "Mingquan Zheng",
                "Michael Lubavin", "Sean Wellington", "Miguel Bautista", "Leonard Sutton");
        assertEquals(expected, testers);
    }

    @Test
    public void testAllCountriesAllDevices() throws Exception {
        Result<?> allDevices = create
                .selectDistinct(DEVICES.DESCRIPTION)
                .from(DEVICES)
                .fetch();
        assertEquals(10, allDevices.size());

        Result<?> allCountries = create
                .selectDistinct(TESTERS.COUNTRY)
                .from(TESTERS)
                .fetch();
        assertEquals(3, allCountries.size());

        Collection<String> selDevices = new ArrayList<>();
        allDevices.forEach(r -> selDevices.add(r.getValue(DEVICES.DESCRIPTION)));

        Collection<String> selCountries = new ArrayList<>();
        allCountries.forEach(r -> selCountries.add(r.getValue(TESTERS.COUNTRY)));

        Result<?> result = privCreate(selDevices, selCountries);
        // marshall result
        List<String> testers = new ArrayList<>();
        result.forEach(r ->
            testers.add(String.join(" ", r.getValue(TESTERS.FIRST_NAME), r.getValue(TESTERS.LAST_NAME)))
        );

        assertEquals(9, testers.size());
        assertEquals(expectedTestersAllCountriesAllDevices, testers);


    }

    @Test
    public void testFasterQueryForAllCountriesAllDevicesSelected() {
        // if all devices selected, can save doing one join
        Result<?> result = privFasterAllCountriesAllDevices();
        // marshall result
        List<String> testers = new ArrayList<>();
        result.forEach(r ->
                testers.add(String.join(" ", r.getValue(TESTERS.FIRST_NAME), r.getValue(TESTERS.LAST_NAME)))
        );
        // same result
        assertEquals(expectedTestersAllCountriesAllDevices, testers);
    }

    private Result<?> privFasterAllCountriesAllDevices() {
        // tester_device left outer join with bugs, count bug_id column in order to sort on it
        // all devices selected, saves one join with devices - NOT needed since we don't need device description
        Result<?> result = create
                .select(count(BUGS.BUG_ID), TESTERS.TESTER_ID, TESTERS.FIRST_NAME, TESTERS.LAST_NAME)
                .from(TESTER_DEVICE)
                .join(TESTERS).on(TESTER_DEVICE.TESTER_ID.equal(TESTERS.TESTER_ID))
                .leftOuterJoin(BUGS).on(TESTER_DEVICE.TESTER_ID.equal(BUGS.TESTER_ID).and(TESTER_DEVICE.DEVICE_ID.equal(BUGS.DEVICE_ID)))
                .groupBy(TESTERS.TESTER_ID)
                .orderBy(inline(1).desc())
                .fetch();
        System.out.println(result);
        return result;
    }
}
