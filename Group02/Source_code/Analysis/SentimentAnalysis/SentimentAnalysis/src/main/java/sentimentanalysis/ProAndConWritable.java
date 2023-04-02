package sentimentanalysis;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProAndConWritable implements Writable {
    private String value1;
    private String value2;

    public ProAndConWritable() {}

    public ProAndConWritable(String value1, String value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(value1);
        out.writeUTF(value2);
    }

    public void readFields(DataInput in) throws IOException {
        value1 = in.readUTF();
        value2 = in.readUTF();
    }

    public String getPro() {
        return value1;
    }

    public String getCon() {
        return value2;
    }
}
