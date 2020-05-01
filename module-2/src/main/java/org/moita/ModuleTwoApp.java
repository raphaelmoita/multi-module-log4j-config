package org.moita;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.moita.domain.Salary;
import org.moita.spark.SparkController;

import java.math.BigDecimal;
import java.util.List;

import static java.math.BigDecimal.*;
import static java.util.Arrays.asList;
import static org.apache.spark.sql.Encoders.DECIMAL;

public class ModuleTwoApp
{
    private final static Logger logger = LogManager.getLogger(ModuleTwoApp.class);

    public BigDecimal calculateSalaryIncrease(Dataset<Salary> salaryDs, BigDecimal increase) {

        logger.debug("Start calculation total salary increase ...");

        BigDecimal totalsalary = salaryDs
                .map(Salary::getValue, DECIMAL())
                .map(s -> s.multiply(increase), DECIMAL())
                .reduce((v1, v2) -> {
                    BigDecimal interimValue = v1.add(v2);

                    logger.info("Interim value is: " + interimValue);

                    return interimValue;
                });

        logger.info("Total salary increase is " + totalsalary);

        return totalsalary;
    }

    public static void main(String[] args) {

        BigDecimal tenPercent = new BigDecimal("1.1");

        List<Salary> salaries = asList(
                Salary.create("A", TEN),
                Salary.create("A", ONE),
                Salary.create("A", ZERO));

        Dataset<Salary> salaryDs = SparkController.createDs(salaries, Salary.class);

        BigDecimal totalSalary = new ModuleTwoApp().calculateSalaryIncrease(salaryDs, tenPercent);

        System.out.println(">> " + totalSalary);
    }
}
