package bdma.bigdata.aiwsbu.data.util;

public class Random {

    static public String getCapitalizedString(int length) {
        String str = getString(length);
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    static public String getCapitalizedString(int min, int max) {
        java.util.Random random = new java.util.Random();
        return getCapitalizedString(min + random.nextInt(max - min));
    }

    static public String getCapitalizedString(int minLength, int maxLength, int minNumber, int maxNumber) {
        StringBuilder buffer = new StringBuilder(getCapitalizedString(minLength, maxLength));
        for (int i = 1; i < getInteger(minNumber, maxNumber); ++i) {
            buffer.append(" ").append(getCapitalizedString(minLength, maxLength));
        }
        return buffer.toString();
    }

    static public int getInteger(int min, int max) {
        java.util.Random random = new java.util.Random();
        return min + random.nextInt(max - min);
    }

    static public String getNumber(int min, int max) {
        java.util.Random random = new java.util.Random();
        int num = min + random.nextInt(max - min);
        return Integer.toString(num);
    }

    static public String getNumber(int min, int max, int size) {
        java.util.Random random = new java.util.Random();
        return String.format("%0" + size + "d", min + random.nextInt(max - min));
    }

    static public String getString(int length) {
        int left = 97; // 'a'
        int right = 122; // 'z'
        java.util.Random random = new java.util.Random();
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < length; i++) {
            buffer.append((char) (left + random.nextInt(right - left)));
        }
        return buffer.toString();
    }

    static public String getString(int min, int max) {
        java.util.Random random = new java.util.Random();
        return getString(min + random.nextInt(max - min));
    }
}
