package org.jooq.example.spring;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.example.db.h2.tables.records.BugsRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.thymeleaf.expression.Lists;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import static org.jooq.example.db.h2.Tables.*;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.inline;

/**
 * Authored by clkim on 3/5/17.
 */

@Controller
public class GreetingController {
    @Autowired
    DSLContext create;


    @GetMapping("/greeting")
    public String greetingForm(Model model) {
        model.addAttribute("greeting", new Greeting());
        initDatabase(); // a hack to populate database tables; should skip if already there
        return "greeting";
    }

    @PostMapping("/greeting")
    public String greetingSubmit(@ModelAttribute Greeting greeting) {
        greeting.setTester(
                findTester(greeting.getCountry(), greeting.getDevice())
        );
        return "result";
    }


    private String findTester(String country, String device) {
        Collection<String> selDevices = new ArrayList<>();
        selDevices.add(device);

        Collection<String> selCountries = new ArrayList();
        selCountries.add(country);

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

        return String.join(", ", testers);
    }

    private void initDatabase() {
        create.truncate(BUGS).execute();
        create.truncate(TESTER_DEVICE).execute();
        create.deleteFrom(DEVICES).execute(); // truncate seems does not work here for H2 because of foreign keys
        create.deleteFrom(TESTERS).execute(); // truncate seems does not work here for H2 because of foreign keys

        try {
            create.loadInto(TESTERS)
                    .loadCSV(new File("src/main/resources/applause/testers.csv"))
                    .fields(TESTERS.TESTER_ID, TESTERS.FIRST_NAME, TESTERS.LAST_NAME, TESTERS.COUNTRY, TESTERS.LAST_LOGIN)
                    .execute();

            create.loadInto(DEVICES)
                    .loadCSV(new File("src/main/resources/applause/devices.csv"))
                    .fields(DEVICES.DEVICE_ID, DEVICES.DESCRIPTION)
                    .execute();

            create.loadInto(TESTER_DEVICE)
                    .loadCSV(new File("src/main/resources/applause/tester_device.csv"))
                    .fields(TESTER_DEVICE.TESTER_ID, TESTER_DEVICE.DEVICE_ID)
                    .execute();

            create.loadInto(BUGS)
                    .loadCSV(new File("src/main/resources/applause/bugs.csv"))
                    .fields(BUGS.BUG_ID, BUGS.DEVICE_ID, BUGS.TESTER_ID)
                    .execute();
        } catch (IOException e) {
            // log it
            e.printStackTrace();
        }
        // smoke test/check
        //Result<BugsRecord> bugs = create.selectFrom(BUGS).fetch();
        //assert 1000 == bugs.size();
    }
}
