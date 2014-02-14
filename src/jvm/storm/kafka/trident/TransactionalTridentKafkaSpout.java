package storm.kafka.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.kafka.Partition;
import storm.trident.spout.IPartitionedTridentSpout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class TransactionalTridentKafkaSpout implements IPartitionedTridentSpout<GlobalPartitionInformation, Partition, Map> {
    private static final String KEY_FIELD = "key";

    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();

    public TransactionalTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }


    @Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new storm.kafka.trident.Coordinator(conf, _config);
    }

    @Override
    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new TridentKafkaEmitter(conf, context, _config, _topologyInstanceId).asTransactionalEmitter();
    }

    @Override
    public Fields getOutputFields() {
        if (_config.keyScheme != null) {
            List<String> fields = new ArrayList<String>();
            fields.addAll(_config.scheme.getOutputFields().toList());
            fields.addAll(_config.keyScheme.getOutputFields().toList());
            return new Fields(fields);
        }
        return _config.scheme.getOutputFields();

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}