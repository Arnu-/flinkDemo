package me.arnu.lru;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 用于实现社群项目的LRU缓存
 */
public class LRUCacheManager<K, V> {

    /**
     * 调用次数
     */
    AtomicLong CallCount = new AtomicLong();

    /**
     * 命中次数
     */
    AtomicLong HitCount = new AtomicLong();

    /**
     * 维护一个map和双向链表
     */
    private final ConcurrentHashMap<K, Node<K, V>> cache; //缓存
    private final int capacity; //容量
    //private int currentSize = 0;
    private Node<K, V> first; //双向链表的头节点
    private Node<K, V> last; //尾节点
    /**
     * 是否总是从数据源获取未命中数据
     */
    private final boolean alwaysGetUnHitValueFromSource;
    /**
     * 数据获取方法
     */
    private final Function<Set<K>, Map<K, V>> valuesGetter;

    /**
     * 根据容量和数据源来初始化
     *
     * @param capacity                      缓存大小
     * @param alwaysGetUnHitValueFromSource 是否总是从数据源获取未命中数据
     */
    public LRUCacheManager(int capacity, boolean alwaysGetUnHitValueFromSource, Function<Set<K>, Map<K, V>> valuesGetter) {
        //currentSize = 0;
        this.capacity = capacity;
        cache = new ConcurrentHashMap<>(capacity);
        this.alwaysGetUnHitValueFromSource = alwaysGetUnHitValueFromSource;
        this.valuesGetter = valuesGetter;
    }

    /**
     * 根据容量和数据源来初始化
     * 默认不总是从数据源获取数据。
     *
     * @param capacity 缓存大小
     */
    public LRUCacheManager(int capacity, Function<Set<K>, Map<K, V>> valuesGetter) {
        //currentSize = 0;
        this.capacity = capacity;
        cache = new ConcurrentHashMap<>(capacity);
        this.alwaysGetUnHitValueFromSource = false;
        this.valuesGetter = valuesGetter;
    }

    /**
     * 1、读取缓存
     * 读取缓存使，如果没有命中，将会进行缓存更新。
     * 如果设置了不同步数据源，则不会取数据源。
     *
     * @param key 读取缓存的Key
     * @return 命中或更新后返回对应值，如果库中没有，则返回空
     */
    public V GetValue(K key) {
        CallCount.getAndIncrement();
        Node<K, V> node = cache.get(key);
        if (node == null) {
            if (!alwaysGetUnHitValueFromSource) {
                return null;
            }
            //缓存未命中,从数据源读取
            V value = UpdateFromSource(key);
            if (value != null) {
                node = new Node<>(key, value);
                cache.put(key, node);
                moveToHead(node);
                return node.getValue();
            } else {
                return null;
            }
        } else {
            HitCount.getAndIncrement();
            //将本次使用的节点移动到头节点
            moveToHead(node);
            return node.getValue();
        }
    }

    /**
     * 将链表当前节点移动到首部
     *
     * @param node 要操作的节点
     */
    private void moveToHead(Node<K, V> node) {
        if (node == null) {
            System.out.println("some thing very wrong!");
            return;
        }
        if (first == null) {
            synchronized (this) {
                if (first == null) {
                    first = last = node;
                }
            }
        } else if (first != node) {
            synchronized (this) {
                if (first != node) {
                    // 新节点
                    if (node.getNext() == null && node.getPre() == null) {
                        first.setPre(node);
                        node.setNext(first);
                        first = node;
                        return;
                    }
                    // 已有节点
                    if (node == last) {
                        node.getPre().setNextToEnd(null);
                        last = node.getPre();
                    } else {
                        // 切出节点
                        node.getPre().setNext(node.getNext());
                        if (node.getNext() == null) {
                            System.out.println("some thing wrong here.");
                        }
                        node.getNext().setPre(node.getPre());
                    }
                    // 节点放到链表首部
                    node.setNext(first);
                    first.setPre(node);
                    first = node;
                    node.setPre(null);
                }
            }
        }
    }

    /**
     * 2、缓存更新
     * 从数据源进行数据更新，
     * 缓存更新时，判断当前缓存数量，如果超出配额，会触发淘汰。
     *
     * @param key 要更新的数据Key
     * @return 如果数据源中没有值，则返回空
     */
    private V UpdateFromSource(K key) {
        if (cache.size() >= capacity) {
            WeedOut();
        }
        Map<K, V> values = valuesGetter.apply(Collections.singleton(key));
        return values != null && values.size() > 0 ? values.get(key) : null;
    }

    /**
     * 外部向缓存中添加元素
     * 此方法必然导致节点状态变成修改过。
     *
     * @param key   添加的key
     * @param value 添加的value
     */
    public void put(K key, V value) {
        Node<K, V> node = cache.get(key);
        if (node == null) {
            synchronized (this) {
                node = cache.get(key);
                if (node == null) {
                    //新增节点
                    node = new Node<>(key, value, true);
                    if (cache.size() >= capacity) {
                        WeedOut();
                    }
                    // 新增节点
                    cache.put(key, node);
                    moveToHead(node);
                }
            }
        } else {
            //修改节点
            synchronized (this) {
                node.setValue(value);
            }
            //使用或修改的节点移动到首部
            moveToHead(node);
        }
    }

    /**
     * 3、缓存淘汰
     * 对数据节点增加修改判断，如果节点数据自缓存以来，没有被使用也没有被修改，按时间排序，选最早的节点进行淘汰。不满足淘汰机制时抛出异常。
     */
    private void WeedOut() {
        if (last.isUpdated()) {
            throw new RuntimeException("不可删除");
        }
        cache.remove(last.getKey());
        removeLast();
    }

    /**
     * 4、节点批量更新
     * 该功能用于触发checkpoint时，节点写入数据库后，批量将节点状态更新为未变更状态，此方法需要锁定所有对节点数据的更新操作。
     *
     * @param syncFun 同步所需要的方法。执行该方法成功表示同步完成，失败则表示同步未完成。
     */
    public void SyncCacheToSource(Function<List<V>, Void> syncFun) {
        List<V> list = new ArrayList<>();
        Node<K, V> cur = first;
        synchronized (this) {
            while (cur != null) {
                if (cur.isUpdated()) {
                    list.add(cur.getValue());
                    cur.setUpdated(false);
                    cur = cur.getNext();
                }
            }
            syncFun.apply(list);
        }
    }

    /**
     * 5、节点批量初始化
     * 该功能用于在checkpoint进行restore时，将checkpoint快照的数据恢复到内存中以备使用。
     *
     * @param caches 要恢复的缓存数据
     */
    public void RestoreCache(Map<K, V> caches) {
        clearCache();
        for (Map.Entry<K, V> entry : caches.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 移除链表最后一个节点
     */
    private void removeLast() {
        if (last != null) {
            last = last.getPre();
            if (last == null) {
                first = null;
            } else {
                last.setNext(null);
            }
            //currentSize--;
        }
    }

    /**
     * 输出缓存大小
     */
    public void printSize() {
        System.out.println("map长度为" + cache.size());
        int len = 0;
        Node<K, V> cur = first;
        while (cur != null) {
            len++;
            cur = cur.getNext();
        }
        System.out.println("链表长度为" + len);
    }

    /**
     * 清空缓存
     */
    private void clearCache() {
        //清空map
        cache.clear();
        //清空链表
        if (first == null) {
            return;
        }
        synchronized (this) {
            if (first.getNext() == null) {
                first = last = null;
                return;
            }
            Node<K, V> curNode = first.getNext();
            while (curNode != null) {
                //GC
                curNode.getPre().setNext(null);
                curNode.setPre(null);
                curNode = curNode.getNext();
            }
            first = last = null;
        }
    }

    /**
     * 检查是否存在缓存中，如果不存在就从数据库更新
     *
     * @param keySet 要检查的缓存的keySet
     */
    public void CheckCacheExists(Set<K> keySet) {
        synchronized (this) {
            Set<K> unCachedKeys = new HashSet<>();
            for (K key : keySet) {
                if (!cache.contains(key)) {
                    unCachedKeys.add(key);
                }
            }
            if (unCachedKeys.size() > 0) {
                Map<K, V> values = valuesGetter.apply(unCachedKeys);
                for (Map.Entry<K, V> kvEntry : values.entrySet()) {
                    if (kvEntry != null && kvEntry.getValue() != null) {
                        Node<K, V> node = new Node<>(kvEntry.getKey(), kvEntry.getValue());
                        node.setPre(last);
                        last.setNext(node);
                        last = node;
                        cache.put(kvEntry.getKey(), node);
                    }
                }
            }
        }
    }
}
