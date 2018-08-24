package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 * @Author: WuYuLong
 * @Date: Create in 16:00 2018/8/24
 * @DESC: 集群支持管道
 */
public class PipelineCluster extends JedisCluster{

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public PipelineCluster(GenericObjectPoolConfig poolConfig, Set<HostAndPort> nodes) {
        super(nodes, poolConfig);
    }
    public PipelineCluster(HostAndPort node) {
        super(node);
    }

    public PipelineCluster(HostAndPort node, int timeout) {
        super(node, timeout);
    }

    public PipelineCluster(HostAndPort node, int timeout, int maxAttempts) {
        super(node, timeout, maxAttempts);
    }

    public PipelineCluster(HostAndPort node, GenericObjectPoolConfig poolConfig) {
        super(node, poolConfig);
    }

    public PipelineCluster(HostAndPort node,  GenericObjectPoolConfig poolConfig, int timeout) {
        super(node, timeout, poolConfig);
    }

    public PipelineCluster(HostAndPort node, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, maxAttempts, poolConfig);
    }

    public PipelineCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public PipelineCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public PipelineCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public PipelineCluster(Set<HostAndPort> nodes) {
        super(nodes);
    }

    public PipelineCluster(Set<HostAndPort> nodes, int timeout) {
        super(nodes, timeout);
    }

    public PipelineCluster(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        super(nodes, timeout, maxAttempts);
    }

    public PipelineCluster(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
        super(nodes, poolConfig);
    }

    public PipelineCluster(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
        super(nodes, timeout, poolConfig);
    }

    public PipelineCluster(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    }

    public PipelineCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public PipelineCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public PipelineCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public Map<String, Object> pipelineCommand(Pipeline pipeline, List<String> keys){
        for (String key : keys){
            pipeline.get(key);
        }
        Map<String, Object> resultMap = new LinkedHashMap<String, Object>();
        List<Object> objects = pipeline.syncAndReturnAll();
        ListIterator<Object> iterator = objects.listIterator();
        while (iterator.hasNext()){
            int i = iterator.previousIndex();
            resultMap.put(keys.get(iterator.nextIndex()), iterator.next());
        }
        return resultMap;
    }

    public <T> T get(final List<String> keys){
        if (null == keys || keys.isEmpty()){
            return null;
        }
        Map<JedisPool, List<String>> jedisPoolMap = getJedisPoolMap(keys);
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (Map.Entry<JedisPool, List<String>> entry : jedisPoolMap.entrySet()){
            JedisPool jedisPool = entry.getKey();
            List<String> subKeys = entry.getValue();
            if (null == subKeys || subKeys.isEmpty()){
                continue;
            }
            Jedis jedis = null;
            Pipeline pipeline = null;
            try {
                jedis = jedisPool.getResource();
                pipeline = jedis.pipelined();
                result.putAll(pipelineCommand(pipeline, subKeys));
            } catch (Exception e){
                e.printStackTrace();
                // TODO
            }finally {
                if (null != jedis){
                    jedis.close();
                }
            }

        }
        return (T)result;
    }

    private Map<JedisPool, List<String>> getJedisPoolMap(List<String> keys){
        Map<JedisPool, List<String>> jedisPoolMap = new LinkedHashMap<JedisPool, List<String>>();
        try {
            for (String key : keys){
                int keySlot = JedisClusterCRC16.getSlot(key);
                JedisPool jedisPoolFromSlot = connectionHandler.getJedisPoolFromSlot(keySlot);
                if (jedisPoolMap.containsKey(jedisPoolFromSlot)){
                    jedisPoolMap.get(jedisPoolFromSlot).add(key);
                } else {
                    List<String> keyList = new ArrayList<>();
                    keyList.add(key);
                    jedisPoolMap.put(jedisPoolFromSlot, keyList);
                }
            }
        } catch (Exception e){
            logger.error(e.getMessage(), e);
        }
        return jedisPoolMap;
    }
}
