package bdma.bigdata.aiwsbu.data.random;

import java.util.Random;

public class Grade {

    private String rowKey = "";

    public Grade(int year, int semester, String student, String course) {
        rowKey = String.format("%4d/%2d/%s/%s", year, semester, student, course);
    }

    public String getRowKey() {
        return rowKey;
    }

    // TODO
    public double generateGrade() {
    	Random r = new Random();
    	double randomValue = 0.1 + (20 - 1) * r.nextDouble();
    	return randomValue;
    }
}
