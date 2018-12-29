package bdma.bigdata.aiwsbu.data;

import bdma.bigdata.aiwsbu.Namespace;
import bdma.bigdata.aiwsbu.data.random.Course;
import bdma.bigdata.aiwsbu.data.random.Instructor;
import bdma.bigdata.aiwsbu.data.random.Student;
import bdma.bigdata.aiwsbu.data.util.Random;

import java.util.*;

public class Test {

    private final TreeMap<String, List<String>> schemaMap = new TreeMap<>();

    private Test() {
        addTable(Namespace.getCourseTable(), Namespace.getCourseFamilies());
        addTable(Namespace.getGradeTable(), Namespace.getGradeFamilies());
        addTable(Namespace.getInstructorTable(), Namespace.getInstructorFamilies());
        addTable(Namespace.getStudentTable(), Namespace.getStudentFamilies());
    }

    public static void main(String[] args) {
        Test setup = new Test();
        setup.run();
    }

    private void addTable(String tableName, String families) {
        List<String> columnFamilies = Arrays.asList(families.split(" "));
        this.schemaMap.put(tableName, columnFamilies);
    }

    private void createTables() {
        System.out.println("Creating Namespace: " + Namespace.get());
        for (String tableName : schemaMap.keySet()) {
            System.out.println("Creating Table: " + tableName);
        }
    }

    private void insertRows() {
        insertRowsCourse();
        insertRowsStudent();
        insertRowsInstructor();
        insertRowsGrade();
    }

    private void insertRowsCourse() {
        System.out.println("Inserting rows to table: " + Namespace.getCourseTable());
        for (Course course : Course.getPool()) {
            System.out.print(course.getRowKey() + ":");
            System.out.print(" #:N:" + course.getName());
            int n = 0;
            for (String instructor : course.getInstructors()) {
                System.out.print(" I:" + (++n) + ":" + instructor);
            }
            System.out.println();
        }
    }

    private void insertRowsGrade() {
        System.out.println("Inserting rows to table: " + Namespace.getGradeTable());
        for (Student student : Student.getPool()) {
            int y = Integer.valueOf(student.getRowKey().substring(0, 4)); // Year
            for (int p = 1; p <= Integer.valueOf(student.getProgram()); ++p) { // Program
                y += p;
                for (int s = p * 2 - 1; s <= p * 2; ++s) { // Semester
                    for (int i = 0; i < Configuration.numberCoursesPerYear / 2; ++i) {
                        String c;
                        String rowKey;
                        String note;
                        c = Course.getInstance().getRowKey().split("/")[0];
                        rowKey = y + "/" + String.format("%02d", s) + student.getRowKey() + "/" + c;
                        System.out.print(rowKey + ":");
                        note = Random.getNumber(0, 2000, 4);
                        System.out.print(" #:G:" + note);
                        System.out.println();
                        c = Course.getInstance(Course.OPTIONAL).getRowKey().split("/")[0];
                        rowKey = y + "/" + String.format("%02d", s) + student.getRowKey() + "/" + c;
                        System.out.print(rowKey + ":");
                        note = Random.getNumber(0, 2000, 4);
                        System.out.print(" #:G:" + note);
                        System.out.println();
                    }
                }
            }
        }
    }

    private void insertRowsInstructor() {
        System.out.println("Inserting rows to table: " + Namespace.getInstructorTable());
        for (Instructor instructor : Instructor.getPool()) {
            Map<Integer, Set<String>> courses = instructor.getCourses();
            if (courses.isEmpty()) {
                continue;
            }
            for (Integer year : courses.keySet()) {
                int n = 0;
                String rowKey = instructor.getName() + "/" + year;
                System.out.print(rowKey + ":");
                for (String course : courses.get(year)) {
                    System.out.print(" #:" + (++n) + ":" + course);
                }
                System.out.println();
            }
        }
    }

    private void insertRowsStudent() {
        System.out.println("Inserting rows to table: " + Namespace.getStudentTable());
        for (Student student : Student.getPool()) {
            System.out.print(student.getRowKey() + ":");
            System.out.print(" #:F:" + student.getFirstName());
            System.out.print(" #:L:" + student.getLastName());
            System.out.print(" #:P:" + student.getProgram());
            System.out.print(" C:B:" + student.getBirthDate());
            System.out.print(" C:D:" + student.getDomicileAddress());
            System.out.print(" C:E:" + student.getEmailAddress());
            System.out.print(" C:P:" + student.getPhoneNumber());
            System.out.println();
        }
    }

    private void run() {
        createTables();
        insertRows();
    }
}
