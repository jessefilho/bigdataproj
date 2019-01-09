package bdma.bigdata.aiwsbu.data;

import static bdma.bigdata.aiwsbu.data.util.Random.getCapitalizedString;

public class Configuration {

    public static final int maxInstructorsPerCourse = 5;
    public static final int numberCoursesPerYear = 20;
    public static final int numberInstructorsTotal = 10;
    public static final int numberStudentsPerYear = 1000;
    public static final int yearStart = 2001;
    public static final int yearStop = 2018;

    private static final int nameLengthMax = 15;
    private static final int nameLengthMin = 2;

    static public String makeName() {
        return getCapitalizedString(Configuration.nameLengthMin, Configuration.nameLengthMax);
    }
}
