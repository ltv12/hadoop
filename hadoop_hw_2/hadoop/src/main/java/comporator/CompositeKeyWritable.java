package comporator;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Lev_Khacheresiantc on 7/22/2016.
 */
public class CompositeKeyWritable
    implements WritableComparable<CompositeKeyWritable> {
    private int cityCode;
    private String operationalSystem;

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(int cityCode, String operationalSystem) {
        this.cityCode = cityCode;
        this.operationalSystem = operationalSystem;
    }

    public int getCityCode() {
        return cityCode;
    }

    public void setCityCode(int cityCode) {
        this.cityCode = cityCode;
    }

    public String getOperationalSystem() {
        return operationalSystem;
    }

    public void setOperationalSystem(String operationalSystem) {
        this.operationalSystem = operationalSystem;
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(cityCode);
        out.writeChars(operationalSystem);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityCode = in.readInt();
        operationalSystem = in.readLine();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CompositeKeyWritable that = (CompositeKeyWritable) o;

        if (cityCode != that.cityCode)
            return false;
        return operationalSystem != null
            ? operationalSystem.equals(that.operationalSystem)
            : that.operationalSystem == null;

    }

    @Override
    public int hashCode() {
        int result = (int) cityCode;
        result = 31 * result
            + (operationalSystem != null ? operationalSystem.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return cityCode + "," + operationalSystem;
    }

    public static class FirstOnlyComparator
        implements RawComparator<CompositeKeyWritable> {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
            int l2) {
            int first1 = WritableComparator.readInt(b1, s1);
            int first2 = WritableComparator.readInt(b2, s2);
            return first1 < first2 ? -1 : first1 == first2 ? 0 : 1;
        }

        public int compare(CompositeKeyWritable x, CompositeKeyWritable y) {
            return x.getCityCode() < y.getCityCode() ? -1
                : x.getCityCode() == y.getCityCode() ? 0 : 1;
        }
    }
}
