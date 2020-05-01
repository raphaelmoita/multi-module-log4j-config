package org.moita.domain;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.math.BigDecimal;

public class Salary implements Serializable {

    private String category;
    private BigDecimal value;

    public static Salary create(String category, BigDecimal value) {
        Salary salary = new Salary();
        salary.setCategory(category);
        salary.setValue(value);

        return salary;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Salary salary = (Salary) o;

        return new EqualsBuilder()
                .append(category, salary.category)
                .append(value, salary.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(category)
                .append(value)
                .toHashCode();
    }
}
