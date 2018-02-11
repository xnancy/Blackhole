package blackhole.base.kafka.server;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

public class KafkaServerOps {

	protected KafkaServerBase ksb;

	public KafkaServerOps(KafkaServerBase ksb) {
		this.ksb = ksb;
	}

	public KafkaServerOps(String host) {
		ksb = new KafkaServerBase(host);
	}

	public boolean createTopic(String topic, Properties props, int noParts, int noRepl) {
		try {
			ZkUtils zkUtils = ksb.getZkUtils();
			AdminUtils.createTopic(zkUtils, topic, noParts, noRepl, props, RackAwareMode.Disabled$.MODULE$);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	public void destroy() {
		ksb.getZkClient().close();
	}
}
