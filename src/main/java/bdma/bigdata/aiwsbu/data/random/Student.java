package bdma.bigdata.aiwsbu.data.random;

import bdma.bigdata.aiwsbu.data.Configuration;
import bdma.bigdata.aiwsbu.data.util.Pool;
import bdma.bigdata.aiwsbu.data.util.Random;

public class Student {

    private static Pool<Student> pool = new Pool<>();

    private String firstName = Configuration.makeName();
    private String lastName = Configuration.makeName();
    private String program;
    private String rowKey;

    private Student(int year, int serial) {
        rowKey = String.format("%04d%06d", year, serial);
    }

    public static Student getInstance() {
        getPool();
        return pool.getRandom();
    }

    public static Pool<Student> getPool() {
        if (pool.isEmpty()) {
            for (int year = Configuration.yearStart; year <= Configuration.yearStop; ++year) {
                for (int serial = 1; serial <= Configuration.numberStudentsPerYear; ++serial) {
                    pool.add(new Student(year, serial).setProgram(year));
                }
            }
        }
        return pool;
    }

    public String getRowKey() {
        return rowKey;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getProgram() {
        return program;
    }

    public String getBirthDate() {
        String dd = Random.getNumber(1, 31, 2);
        String mm = Random.getNumber(1, 12, 2);
        String yyyy = Random.getNumber(1900, 2018 - Integer.valueOf(program) - 16);
        return "" + dd + "/" + mm + "/" + yyyy;
    }

    public String getDomicileAddress() {
        StringBuilder address = new StringBuilder(Random.getNumber(1, 999));
        for (int i = 1; i < Random.getInteger(2, 5); ++i) {
            address.append(" ").append(Random.getCapitalizedString(5, 8));
        }
        return address.toString();
    }

    public String getEmailAddress() {
        return "";
    }

    public String getPhoneNumber() {
        return "0" + Random.getNumber(111111111, 999999999);
    }

    private Student setProgram(int year) {
        int p = Random.getInteger(1, 5);
        if (year + p > 2018) {
            program = "" + (2018 - year + 1);
        } else {
            program = "" + p;
        }
        return this;
    }
}
