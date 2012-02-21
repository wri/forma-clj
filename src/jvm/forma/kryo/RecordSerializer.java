package forma.kryo;

import com.esotericsoftware.kryo.Serializer;

import java.nio.ByteBuffer;

/** User: sritchie Date: 2/19/12 Time: 6:56 PM */
public class RecordSerializer extends Serializer {
    @Override public void writeObjectData(ByteBuffer byteBuffer, Object o) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override public <T> T readObjectData(ByteBuffer byteBuffer, Class<T> tClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
