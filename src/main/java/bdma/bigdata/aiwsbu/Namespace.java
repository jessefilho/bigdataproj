package bdma.bigdata.aiwsbu;

import org.apache.hadoop.hbase.TableName;



public class Namespace {

    private static final String ID = "A_21805893"; // TODO
    private static final String courseFamilies = "# I";
    private static final String courseTable = "C";
    private static final String gradeFamilies = "#";
    private static final String gradeTable = "G";
    private static final String instructorFamilies = "#";
    private static final String instructorTable = "I";
    private static final String studentFamilies = "# C";
    private static final String studentTable = "S";

    public static String get() {
        return ID;
    }

    public static String getCourseFamilies() {
        return courseFamilies;
    }

    public static String getCourseTable() {
        return courseTable;
    }

    public static TableName getCourseTableName() {
        return TableName.valueOf(ID, courseTable);
    }

    public static String getGradeFamilies() {
        return gradeFamilies;
    }

    public static String getGradeTable() {
        return gradeTable;
    }

    public static TableName getGradeTableName() {
        return TableName.valueOf(ID, gradeTable);
    }

    public static String getInstructorFamilies() {
        return instructorFamilies;
    }

    public static String getInstructorTable() {
        return instructorTable;
    }

    public static TableName getInstructorTableName() {
        return TableName.valueOf(ID, instructorTable);
    }

    public static String getStudentFamilies() {
        return studentFamilies;
    }

    public static String getStudentTable() {
        return studentTable;
    }

    public static TableName getStudentTableName() {
        return TableName.valueOf(ID, studentTable);
    }

    public static TableName getTableName(String table) {
        return TableName.valueOf(ID, table);
    }
}
