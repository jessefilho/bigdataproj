package bdma.bigdata.aiwsbu.data.random;

import bdma.bigdata.aiwsbu.data.Configuration;
import bdma.bigdata.aiwsbu.data.util.Pool;
import bdma.bigdata.aiwsbu.data.util.Random;

import java.util.Set;
import java.util.TreeSet;

public class Course {

    public static final char OPTIONAL = 'B';
    private static Pool<Course> pool = new Pool<Course>();
    private static Pool<Integer> poolIndexA = new Pool<Integer>();
    private static Pool<Integer> poolIndexB = new Pool<Integer>();

    private Set<String> instructors = new TreeSet<String>();
    private String name = Random.getCapitalizedString(1, 15, 1, 5);
    private String rowKey;

    private Course(int semester, char type, int serial, int year) {
        rowKey = String.format("S%02d%s%03d/%04d", semester, type, serial, 9999 - year);
    }

    public static Course getInstance() {
        return getInstance('A');
    }

    public static Course getInstance(char type) {
        getPool();
        if (type != OPTIONAL) {
            return pool.get(poolIndexA.getRandom());
        } else {
            return pool.get(poolIndexB.getRandom());
        }
    }

    public static Pool<Course> getPool() {
        int index = 0;
        if (poolIndexA.isEmpty()) {
            for (int s = 0; s < 10; ++s) {
                int semester = s + 1;
                for (int i = 0; i < Configuration.numberCoursesPerYear / 2; ++i) {
                    int serial = i + 1;
                    for (int n = 0; n < Random.getInteger(0, 5); ++n) {
                        int year = Random.getInteger(Configuration.yearStart, Configuration.yearStop);
                    	pool.add(new Course(semester, 'A', serial, year).setInstructors(year));
                        poolIndexA.add(index++);
//                        for (int year = Configuration.yearStart; year < Configuration.yearStop+1; year++) {
//                        	pool.add(new Course(semester, 'A', serial, year).setInstructors(year));
//                            poolIndexA.add(index++);
//						}
                        
                    }
                }
            }
        }
        if (poolIndexB.isEmpty()) {
            for (int s = 0; s < 10; ++s) {
                int semester = s + 1;
                for (int i = 0; i < Configuration.numberCoursesPerYear / 2 * 5; ++i) {
                    int serial = i + 1;
                    for (int n = 0; n < Random.getInteger(0, 5); ++n) {
                        int year = Random.getInteger(Configuration.yearStart, Configuration.yearStop);
                        pool.add(new Course(semester, 'B', serial, year).setInstructors(year));
                        poolIndexB.add(index++);
                    }
                }
            }
        }
        return pool;
    }

    public String getRowKey() {
        return rowKey;
    }

    public Set<String> getInstructors() {
        return instructors;
    }

    public String getName() {
        return name;
    }

    private Course setInstructors(int year) {
        if (instructors.isEmpty()) {
            for (int i = 0; i < Random.getInteger(1, Configuration.maxInstructorsPerCourse); ++i) {
                Instructor instructor = Instructor.getInstance();
                instructors.add(instructor.getName());
                instructor.addCourse(name, year);
            }
        }
        return this;
    }
}
