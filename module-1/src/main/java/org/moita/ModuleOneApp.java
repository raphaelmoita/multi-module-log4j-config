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

public class ModuleOneApp
{
    private final static Logger logger = LogManager.getLogger(ModuleOneApp.class);

    public BigDecimal calculateTotalSalary(Dataset<Salary> salaryDs) {

        logger.debug("Start calculation total salary ...");

        BigDecimal totalSalary = salaryDs.map(Salary::getValue, DECIMAL())
                .reduce((v1, v2) -> {
                    BigDecimal interimValue = v1.add(v2);

                    logger.info("Interim value is: " + interimValue);

                    return interimValue;
                });

        logger.info("Total salary is " + totalSalary);

        return totalSalary;
    }

    public static void main(String[] args) {

        List<Salary> salaries = asList(
                Salary.create("A", TEN),
                Salary.create("A", ONE),
                Salary.create("A", ZERO));

        Dataset<Salary> salaryDs = SparkController.createDs(salaries, Salary.class);

        BigDecimal totalSalary = new ModuleOneApp().calculateTotalSalary(salaryDs);

        System.out.println(">> " + totalSalary);
    }
}
