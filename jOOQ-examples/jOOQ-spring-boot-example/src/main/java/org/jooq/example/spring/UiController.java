package org.jooq.example.spring;

import com.google.common.base.Joiner;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.jooq.example.db.h2.Tables.*;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.inline;

/**
 * Authored by clkim on 3/11/17.
 */
@RestController
public class UiController {
    @Autowired
    DSLContext create;

    @RequestMapping(value = "/testers/{countries}", method = RequestMethod.GET)
    //  this is a bit of a hack to concatenate a query string at end
    //  TODO seems better to pass in named query request parameters https://spring.io/guides/gs/rest-service/
    public Map<String, Object> matchTesters(@PathVariable("countries") String countriesCsv) {
        Map<String,Object> model = new HashMap<>();
        // find testers
        model.put("testers", findTesters(countriesCsv, "")); // hard-code 3 device types for now
        return model;
    }

    @RequestMapping("/allcountries")
    public Map<String, Object> countries() {
        Map<String,Object> model = new HashMap<>();
        model.put("countries", allCountriesCsv().countriesCsv);
        model.put("flags", allCountriesCsv().flagsCsv);
        return model;
    }


    private String findTesters(String countriesCsv, String device) {
        // TODO pass in selected device types similarly to country names
        Collection<String> selDevices = Arrays.asList("Droid DNA, iPhone 4, Galaxy S4".split("\\s*,\\s*"));

        Collection<String> selCountries = Arrays.asList(countriesCsv.split("\\s*,\\s*"));

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

        // marshall result
        List<String> testers = new ArrayList<>();
        result.forEach(r -> {
            testers.add(String.join(" ", r.getValue(TESTERS.FIRST_NAME), r.getValue(TESTERS.LAST_NAME)));
        });

        return String.join(", ", testers);
    }

    private CsvPair allCountriesCsv() {
        Result<?> allCountries = create
                .selectDistinct(TESTERS.COUNTRY)
                .from(TESTERS)
                .fetch();

        Collection<String> countriesList = new ArrayList<>();
        Collection<String> flagsList = new ArrayList<>();
        allCountries.forEach(r -> {
            String s = r.getValue(TESTERS.COUNTRY);
            countriesList.add(s);
            flagsList.add("<img src='flags/1x1/" + s + ".svg' />"); // from https://github.com/lipis/flag-icon-css
        });
        String countriesCsv = Joiner.on(',').join(countriesList);
        String flagsCsv = Joiner.on(',').join(flagsList);
        return new CsvPair(countriesCsv, flagsCsv);
    }

    private static class CsvPair {
        String countriesCsv;
        String flagsCsv;

        CsvPair(String countriesCsv, String flagsCsv) {
            this.countriesCsv = countriesCsv;
            this.flagsCsv = flagsCsv;
        }
    }
}
