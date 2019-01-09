package bdma.bigdata.aiwsbu.data;

import bdma.bigdata.aiwsbu.Namespace;
import bdma.bigdata.aiwsbu.data.random.Course;
import bdma.bigdata.aiwsbu.data.random.Instructor;
import bdma.bigdata.aiwsbu.data.random.Student;
import bdma.bigdata.aiwsbu.data.util.Random;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class Setup {

    private final TreeMap<TableName, List<String>> schemaMap = new TreeMap<>();

    private Connection connection = null;

    private Setup() {
        try {
            connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        } catch (IOException e) {
            System.err.println("Failed to connect to HBase.");
            System.exit(0);
        }
        addTable(Namespace.getCourseTableName(), Namespace.getCourseFamilies());
        addTable(Namespace.getGradeTableName(), Namespace.getGradeFamilies());
        addTable(Namespace.getInstructorTableName(), Namespace.getInstructorFamilies());
        addTable(Namespace.getStudentTableName(), Namespace.getStudentFamilies());
    }

    public static void main(String[] args) {
        Setup setup = new Setup();
        setup.run();
    }

    private void addTable(TableName tableName, String families) {
        List<String> columnFamilies = Arrays.asList(families.split(" "));
        this.schemaMap.put(tableName, columnFamilies);
    }

    private void createTables() {
        try (Admin admin = connection.getAdmin()) {
            try {
                admin.createNamespace(NamespaceDescriptor.create(Namespace.get()).build());
            } catch (Exception ignored) {
            }
            for (TableName tableName : schemaMap.keySet()) {
                try {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                } catch (Exception ignored) {
                }
                HTableDescriptor htd = new HTableDescriptor(tableName);
                for (String familyName : schemaMap.get(tableName)) {
                    htd.addFamily(new HColumnDescriptor(familyName));
                }
                try {
                    admin.createTable(htd);
                    System.out.println("Table created: " + tableName);
                } catch (Exception e) {
                    System.err.println("Failed to create table: " + tableName);
                }
            }
        } catch (Exception e) {
            System.err.println("Unknown connection error.");
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
        try (Table table = connection.getTable(Namespace.getCourseTableName())) {
            for (Course course : Course.getPool()) {
                Put put = new Put(Bytes.toBytes(course.getRowKey()));
                put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("N"), Bytes.toBytes(course.getName()));
                int n = 0;
                for (String instructor : course.getInstructors()) {
                    put.addImmutable(Bytes.toBytes("I"), Bytes.toBytes(++n), Bytes.toBytes(instructor));
                }
                table.put(put);
            }
        } catch (Exception e) {
            System.err.println("Failed to open table: " + Namespace.getCourseTableName());
        }
    }

    private void insertRowsGrade() {
        System.out.println("Inserting rows to table: " + Namespace.getGradeTable());
        try (Table table = connection.getTable(Namespace.getGradeTableName())) {
            for (Student student : Student.getPool()) {
                int y = Integer.valueOf(student.getRowKey().substring(0, 4)); // Year
                for (int p = 1; p <= Integer.valueOf(student.getProgram()); ++p) { // Program
                    y += p;
                    for (int s = p * 2 - 1; s <= p * 2; ++s) { // Semester
                        for (int i = 0; i < Configuration.numberCoursesPerYear / 2; ++i) {
                            String c;
                            String rowKey;
                            String note;
                            Put put;
                            c = Course.getInstance().getRowKey().split("/")[0];
                            rowKey = y + "/" + String.format("%02d", s) + student.getRowKey() + "/" + c;
                            put = new Put(Bytes.toBytes(rowKey));
                            note = Random.getNumber(0, 2000, 4);
                            put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("G"), Bytes.toBytes(note));
                            table.put(put);
                            c = Course.getInstance(Course.OPTIONAL).getRowKey().split("/")[0];
                            rowKey = y + "/" + String.format("%02d", s) + student.getRowKey() + "/" + c;
                            put = new Put(Bytes.toBytes(rowKey));
                            note = Random.getNumber(0, 2000, 4);
                            put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("G"), Bytes.toBytes(note));
                            table.put(put);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to open table: " + Namespace.getGradeTableName());
        }
    }

    private void insertRowsInstructor() {
        System.out.println("Inserting rows to table: " + Namespace.getInstructorTable());
        try (Table table = connection.getTable(Namespace.getInstructorTableName())) {
            for (Instructor instructor : Instructor.getPool()) {
                Map<Integer, Set<String>> courses = instructor.getCourses();
                if (courses.isEmpty()) {
                    continue;
                }
                for (Integer year : courses.keySet()) {
                    int n = 0;
                    String rowKey = instructor.getName() + "/" + year;
                    Put put = new Put(Bytes.toBytes(rowKey));
                    for (String course : courses.get(year)) {
                        put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes(++n), Bytes.toBytes(course));
                    }
                    table.put(put);
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to open table: " + Namespace.getInstructorTableName());
        }
    }

    private void insertRowsStudent() {
        System.out.println("Inserting rows to table: " + Namespace.getStudentTable());
        try (Table table = connection.getTable(Namespace.getStudentTableName())) {
            for (Student student : Student.getPool()) {
                Put put = new Put(Bytes.toBytes(student.getRowKey()));
                put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("F"), Bytes.toBytes(student.getFirstName()));
                put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("L"), Bytes.toBytes(student.getLastName()));
                put.addImmutable(Bytes.toBytes("#"), Bytes.toBytes("P"), Bytes.toBytes(student.getProgram()));
                put.addImmutable(Bytes.toBytes("C"), Bytes.toBytes("B"), Bytes.toBytes(student.getBirthDate()));
                put.addImmutable(Bytes.toBytes("C"), Bytes.toBytes("D"), Bytes.toBytes(student.getDomicileAddress()));
                put.addImmutable(Bytes.toBytes("C"), Bytes.toBytes("E"), Bytes.toBytes(student.getEmailAddress()));
                put.addImmutable(Bytes.toBytes("C"), Bytes.toBytes("P"), Bytes.toBytes(student.getPhoneNumber()));
                table.put(put);
            }
        } catch (Exception e) {
            System.err.println("Failed to open table: " + Namespace.getStudentTableName());
        }
    }

    private void run() {
        createTables();
        insertRows();
        System.out.println("Done!");
    }
}
