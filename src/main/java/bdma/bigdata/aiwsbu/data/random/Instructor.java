package bdma.bigdata.aiwsbu.data.random;

import bdma.bigdata.aiwsbu.data.Configuration;
import bdma.bigdata.aiwsbu.data.util.Pool;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class Instructor {

    private static Pool<Instructor> pool = new Pool<>();

    private Map<Integer, Set<String>> courses = new TreeMap<>();
    private String name = Configuration.makeName() + " " + Configuration.makeName();

    private Instructor() {
    }

    public static Instructor getInstance() {
        getPool();
        return pool.getRandom();
    }

    public static Pool<Instructor> getPool() {
        if (pool.isEmpty()) {
            for (int i = 0; i < Configuration.numberInstructorsTotal; ++i) {
                Instructor instructor = new Instructor();
                pool.add(instructor);
            }
        }
        return pool;
    }

    public Map<Integer, Set<String>> getCourses() {
        return courses;
    }

    public String getName() {
        return name;
    }

    public void addCourse(String course, int year) {
        if (!courses.containsKey(year)) {
            courses.put(year, new TreeSet<>());
        }
        courses.get(year).add(course);
    }
}
