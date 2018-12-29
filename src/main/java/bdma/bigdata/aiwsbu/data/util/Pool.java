package bdma.bigdata.aiwsbu.data.util;

import java.util.ArrayList;

public class Pool<E> extends ArrayList<E> {

    public E getRandom() {
        return super.get(Random.getInteger(0, this.size() - 1));
    }
}
