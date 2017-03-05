package org.jooq.example.spring;

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
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

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
                .loadCSV(new File("src/main/resources/applause/bugs20test.csv"))
                //.loadCSV(new File("src/main/resources/applause/bugs.csv"))
                .fields(BUGS.BUG_ID, BUGS.DEVICE_ID, BUGS.TESTER_ID)
                .execute();
        Result<BugsRecord> bugs = create.selectFrom(BUGS).fetch();
        assertEquals(20, bugs.size());
        //assertEquals(1000, bugs.size());
    }

    @Test
    public void testMultipleCountriesDevices() throws Exception {
        Result<BugsRecord> bugs = create.selectFrom(BUGS).fetch();
        assertEquals(20, bugs.size());
        //assertEquals(1000, bugs.size());

        Collection<String> selDevices = new ArrayList<>();
        selDevices.add("Droid DNA");
        selDevices.add("iPhone 4");
        selDevices.add("Galaxy S4");

        Collection<String> selCountries = new ArrayList();
        selCountries.add("GB");
        selCountries.add("US");

        Result<?> result1 = create
                .select(count(), TESTERS.TESTER_ID, TESTERS.FIRST_NAME, TESTERS.LAST_NAME)
                .from(BUGS)
                .join(DEVICES).on(DEVICES.DESCRIPTION.in(selDevices))
                .join(TESTERS).on(TESTERS.COUNTRY.in(selCountries))
                .where(BUGS.DEVICE_ID.equal(DEVICES.DEVICE_ID).and(BUGS.TESTER_ID.equal(TESTERS.TESTER_ID)))
                .groupBy(TESTERS.TESTER_ID)
                .orderBy(inline(1).desc())
                .fetch();
        System.out.println(result1);
        assertEquals(3, result1.size());
        //assertEquals(5, result1.size());

        // look for tester-device meeting country and device selections
        //  if not submitted bugs, tester won't be in result1 above
        Result<?> result2 = create
                .select(TESTERS.TESTER_ID, TESTERS.FIRST_NAME, TESTERS.LAST_NAME)
                .from(TESTER_DEVICE)
                .join(DEVICES).on(DEVICES.DESCRIPTION.in(selDevices))
                .join(TESTERS).on(TESTERS.COUNTRY.in(selCountries))
                .where(TESTER_DEVICE.TESTER_ID.equal(TESTERS.TESTER_ID).and(TESTER_DEVICE.DEVICE_ID.equal(DEVICES.DEVICE_ID)))
                .groupBy(TESTERS.TESTER_ID)
                .fetch();
        System.out.println(result2);
        assertEquals(5, result2.size());

        // marshall answer 
        Set<Integer> seenIds = new HashSet<>();
        Deque<String> testers = new LinkedList<>();
        result1.forEach(r -> {
            seenIds.add(r.getValue(TESTERS.TESTER_ID));
            testers.add(String.join(" ", r.getValue(TESTERS.FIRST_NAME), r.getValue(TESTERS.LAST_NAME)));
        });
        // add to end of list the testers who match Country and Device but have no matching bug reports
        result2.forEach(r -> {
            if (!seenIds.contains(r.getValue(TESTERS.TESTER_ID))) {
                testers.add(String.join(" ", r.getValue(TESTERS.FIRST_NAME), r.getValue(TESTERS.LAST_NAME)));
            }
        });

        testers.forEach(s -> System.out.println(s));
        assertEquals(5, testers.size());
        assertEquals("Darshini Thiagarajan", testers.getFirst());
        //assertEquals("Taybin Rutkin", testers.getFirst());
        assertEquals("Leonard Sutton", testers.getLast());
    }

    //TODO - "All" country use case -- probably faster for both result1 and result2 TESTERS joins to just join on TESTER_ID
}
