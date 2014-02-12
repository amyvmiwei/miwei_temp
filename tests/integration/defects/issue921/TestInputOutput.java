
import java.nio.ByteBuffer;
import org.apache.hadoop.io.Text;
import org.hypertable.thrift.*;
import org.hypertable.thriftgen.*;
import org.hypertable.hadoop.mapred.*;
import org.hypertable.hadoop.mapreduce.*;

public class TestInputOutput {
  public static void main(String args[]) {
    // add a few cells 
    try {
      HypertableRecordWriter hrw = new HypertableRecordWriter("test", "test");
      hrw.write(new Text("row1\tcf1"), new Text("value"));
      hrw.write(new Text("row2\tcf1"), new Text("value\nnewline"));
      hrw.write(new Text("row3\tcf1"), new Text("value\nnew\nline"));
      hrw.write(new Text("row4\tcf1"), new Text("value\nnew\nli\\ne\0"));
      hrw.write(new Text("row5\tcf1"), new Text("value\\backslash"));
      hrw.close(null);
    }
    catch (java.io.IOException exception) {
      System.out.println("failure");
      System.exit(-1);
    }

    try {
      Text key = new Text();
      Text value = new Text();
      ThriftClient client = ThriftClient.create("localhost", 15867);
      // now read them with no_escape = TRUE
      System.out.println("hypertable.mapreduce.input.no_escape = true");
      HypertableRecordReader hrr = new HypertableRecordReader(client, "test",
            "test", new org.hypertable.hadoop.mapreduce.ScanSpec(), false,
            true);
      while (hrr.next(key, value) == true)
        System.out.println(key + "\t" + value);
      hrr.close();

      // now read them with no_escape = FALSE
      System.out.println("hypertable.mapreduce.input.no_escape = false");
      hrr = new HypertableRecordReader(client, "test",
            "test", new org.hypertable.hadoop.mapreduce.ScanSpec(), false,
            false);
      while (hrr.next(key, value) == true)
        System.out.println(key + "\t" + value);
      hrr.close();
    }
    catch (Exception exception) {
      System.out.println("failure");
      System.exit(-1);
    }

    System.out.println("success");
  }
};
