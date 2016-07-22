package customtypes;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lev on 15.07.16.
 */
public class IpInfoWritable implements Writable {
    private int totalBytes;
    private Double average;

    public IpInfoWritable() {
    }

    public IpInfoWritable(double average, int totalBytes) {
        this.totalBytes = totalBytes;
        this.average = average;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(totalBytes);
        dataOutput.writeDouble(average);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.totalBytes = dataInput.readInt();
        this.average = dataInput.readDouble();
    }

    public int getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(int totalBytes) {
        this.totalBytes = totalBytes;
    }

    public Double getAverage() {
        return average;
    }

    public void setAverage(Double average) {
        this.average = average;
    }

    @Override
    public String toString() {
        return String.format("%.2f,%d", getAverage(), getTotalBytes());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IpInfoWritable that = (IpInfoWritable) o;

        if (totalBytes != that.totalBytes)
            return false;
        return average != null ? average.equals(that.average)
            : that.average == null;

    }

    @Override
    public int hashCode() {
        int result = totalBytes;
        result = 31 * result + (average != null ? average.hashCode() : 0);
        return result;
    }
}
