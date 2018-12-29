package bdma.bigdata.aiwsbu.data;

import static bdma.bigdata.aiwsbu.data.util.Random.getCapitalizedString;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

import com.github.javafaker.Faker;



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

 // TODO
    public String generateClass() {
    	int randomNum = ThreadLocalRandom.current().nextInt(1, 5);
        return String.valueOf(randomNum);
    }
    static Faker person = new Faker(new Locale("en"));

    // TODO
    public static String generateFirstName() {     	
        return person.name().firstName();
    }

    // TODO
    public static String generateLastName() {
        return person.name().lastName();
    }

    

    // TODO
    public static String generateDomicileAddress() {
    	
    	String numberAddr = person.address().buildingNumber();
    	if (numberAddr == "") {
    		numberAddr = "54" ;
    	}
        return numberAddr+", "+person.address().streetName()+", "+person.address().cityName()+", "+person.address().country();
    }

    // TODO
    public static String generateEmailAddress() {
        return person.internet().emailAddress();
    }

    // TODO
    public static String generatePhoneNumber() {
        return person.phoneNumber().phoneNumber();
    }
}
