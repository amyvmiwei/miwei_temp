
import java.nio.ByteBuffer;
import org.hypertable.thrift.*;
import org.hypertable.thriftgen.*;

public class TestSerializers {
  public static void main(String args[]) {
    // add a cell 
    SerializedCellsWriter scw = new SerializedCellsWriter(1);
    ByteBuffer value = ByteBuffer.allocate(1024);
    assert scw.add("rowkey", "column_family", "column_qualifier", 12387123,
            value) == true;
    assert scw.add("rowkey", "column_family", "column_qualifier", 12387123,
            value) == false;
    scw.finalize((byte)0);
    assert scw.add("rowkey", "column_family", "column_qualifier", 12387123,
            value) == true;
    assert scw.add("rowkey", "column_family", "column_qualifier", 12387123,
            value) == false;
    scw.clear();
    assert scw.add("rowkey", "column_family", "column_qualifier", 12387123,
            value) == true;
    assert scw.add("rowkey", "column_family", "column_qualifier", 12387123,
            value) == false;
    System.out.println("success");
  }
};
