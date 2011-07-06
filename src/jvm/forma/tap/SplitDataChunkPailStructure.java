package forma.tap;

import java.util.ArrayList;
import java.util.List;
import forma.schema.DataChunk;
import forma.schema.DataValue;
import forma.schema.LocationProperty;
import forma.schema.LocationPropertyValue;
import forma.schema.ModisChunkLocation;

public class SplitDataChunkPailStructure extends DataChunkPailStructure {
    
    @Override
    public List<String> getTarget(DataChunk t) {
        List<String> ret = new ArrayList<String>();

        ret.add(t.getDataset());
        return ret;
    }
}
