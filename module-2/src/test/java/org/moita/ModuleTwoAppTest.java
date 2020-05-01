package org.moita;

import org.apache.spark.sql.Dataset;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.moita.domain.Salary;
import org.moita.spark.SparkController;

import java.math.BigDecimal;
import java.util.List;

import static java.math.BigDecimal.*;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;

class ModuleTwoAppTest {

    private ModuleTwoApp moduleOneApp = new ModuleTwoApp();

    @Test
    public void shouldCalculateTotalSalary() {

        BigDecimal tenPercent = new BigDecimal("1.1");
        BigDecimal expectedTotalSalary = new BigDecimal("12.1");

        List<Salary> salaries = asList(
                Salary.create("A", TEN),
                Salary.create("A", ONE),
                Salary.create("A", ZERO));

        Dataset<Salary> salaryDs = SparkController.createDs(salaries, Salary.class);

        BigDecimal totalSalary = moduleOneApp.calculateSalaryIncrease(salaryDs, tenPercent);

        assertThat(expectedTotalSalary,  Matchers.comparesEqualTo(totalSalary));
    }
}