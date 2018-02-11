package blackhole.base.kafka.server;

import java.util.Collection;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * TODO Programmatically configure and launch Zookeeper + Kafka.
 * XXX For now, this class is just sample code of interaction with Kafka Server
 * @author madassar
 *
 */
public class KafkaServerBase
{
	protected final Logger log = LogManager.getLogger(getClass());
	
	protected String zookeeperHosts;
	ZkClient zkClient;
    ZkUtils zkUtils;
    
    // Should not directly instantiate, except through KafkaServerOps?
    protected KafkaServerBase(Collection<String> hosts) {
    		this(String.join(",", hosts));
    }
	
	public KafkaServerBase(String hosts) {
		this.zookeeperHosts = hosts;
		int sessionTimeOutInMs = 15 * 1000; // 15 secs
        int connectionTimeOutInMs = 10 * 1000; // 10 secs
        
        zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
        zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);
	}
	
	public ZkUtils getZkUtils() {
		return zkUtils;
	}
	
	public ZkClient getZkClient() {
		return zkClient;
	}
	
    public static void main(String[] args) throws Exception {
    		System.setProperty("zookeeper.jmx.log4j.disable", "true");
    		KafkaServerOps ops = new KafkaServerOps("127.0.0.1:2181");
        ops.createTopic("testTopic", new Properties(), 2, 1);
    		ops.destroy();
    }
}