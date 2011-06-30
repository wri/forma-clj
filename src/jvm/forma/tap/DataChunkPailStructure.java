package forma.tap;

import forma.schema.DataChunk;

public class DataChunkPailStructure extends ThriftPailStructure<DataChunk> {

    @Override
    protected DataChunk createThriftObject() {
        return new DataChunk();
    }

    public Class getType() {
        return DataChunk.class;
    }
}
