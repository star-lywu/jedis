package redis.clients.jedis.pipline;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.PipelineCluster;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: WuYuLong
 * @Date: Create in 17:56 2018/8/24
 * @DESC:
 */
public class PiplineClusterTest {

    private PipelineCluster pipelineCluster;

    private List<String> keys = new ArrayList<String>();

    @Before
    public void before() {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxWaitMillis(2 * 1000L);
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();

        //test mHgetAll  10.1.1.41:6379,10.1.1.41:7379,10.1.1.41:8379
        nodes.add(new HostAndPort("10.1.1.41", 6379));
        nodes.add(new HostAndPort("10.1.1.41", 7379));
        nodes.add(new HostAndPort("10.1.1.41", 8379));
        pipelineCluster = new PipelineCluster(poolConfig, nodes);
        JedisCluster jedisCluster = new JedisCluster(nodes, poolConfig);
        for (int i = 1; i < 10; i++) {
            String key = "key:" + i;
            jedisCluster.set(key, key);
            keys.add(key);
        }
    }

    @Test
    public void testMget(){
        Map<String, Object> resultMap = pipelineCluster.get(keys);
        Assert.assertNotNull(resultMap);
    }
}
