package IdAndCount;

import java.util.Comparator;

/**
 * Created by Lev_Khacheresiantc on 7/25/2016.
 */
public class IdAndCount {
    public static Comparator<IdAndCount> COMPARATOR_BY_ID =
        new Comparator<IdAndCount>() {
            @Override
            public int compare(IdAndCount o1, IdAndCount o2) {
                return o1.getId().compareTo(o2.getId());
            }
        };
    public static Comparator<IdAndCount> COMPARATOR_BY_COUNT =
        new Comparator<IdAndCount>() {
            @Override
            public int compare(IdAndCount o1, IdAndCount o2) {
                return Integer.compare(o1.getCount(), o2.getCount());
            }
        };
    int count;
    String id;

    public IdAndCount(String id, int count) {
        this.count = count;
        this.id = id;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IdAndCount that = (IdAndCount) o;

        return id.equals(that.id);

    }

    @Override
    public String toString() {
        return id + " " + count;
    }

    @Override
    public int hashCode() {
        int result = count;
        result = 31 * result + id.hashCode();
        return result;
    }
}
