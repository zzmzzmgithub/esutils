package com.wirktop.esutils;

/**
 * @author Cosmin Marginean
 */
public class TestPojo {

    private String id;
    private String name;
    private String gender;
    private int age;
    private double height;

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public double getHeight() {
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestPojo testPojo = (TestPojo) o;

        if (age != testPojo.age) return false;
        if (Double.compare(testPojo.height, height) != 0) return false;
        if (id != null ? !id.equals(testPojo.id) : testPojo.id != null) return false;
        if (name != null ? !name.equals(testPojo.name) : testPojo.name != null) return false;
        return gender != null ? gender.equals(testPojo.gender) : testPojo.gender == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + age;
        temp = Double.doubleToLongBits(height);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
