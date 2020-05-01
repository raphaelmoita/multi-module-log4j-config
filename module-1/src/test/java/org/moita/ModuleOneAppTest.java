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
import static org.junit.jupiter.api.Assertions.assertEquals;

class ModuleOneAppTest {

    private ModuleOneApp moduleOneApp = new ModuleOneApp();

    @Test
    public void shouldCalculateTotalSalary() {

        BigDecimal expectedTotalSalary = new BigDecimal("11");

        List<Salary> salaries = asList(
                Salary.create("A", TEN),
                Salary.create("A", ONE),
                Salary.create("A", ZERO));

        Dataset<Salary> salaryDs = SparkController.createDs(salaries, Salary.class);

        BigDecimal totalSalary = moduleOneApp.calculateTotalSalary(salaryDs);

        assertThat(expectedTotalSalary,  Matchers.comparesEqualTo(totalSalary));
    }
}