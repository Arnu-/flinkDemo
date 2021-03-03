package me.arnu.lru;

public class Node<K, V> {

    private boolean isUpdated;
    private final K key;
    private V value;
    private Node<K, V> pre;
    private Node<K, V> next;

    public Node(K key, V value, boolean isUpdate) {
        super();
        this.key = key;
        this.value = value;
        this.isUpdated = isUpdate;
    }

    public K getKey() {
        return key;
    }

    public Node(K key, V value) {
        this(key, value, false);
    }

    @Override
    public String toString() {
        return "Node{" +
                "key=" + key +
                '}';
    }

    public Node<K, V> getPre() {
        return pre;
    }

    public void setPre(Node<K, V> pre) {
//        if (this.pre != null && pre == null) {
//            System.out.println("移动到了队首");
//        }
        this.pre = pre;
    }

    public Node<K, V> getNext() {
        return next;
    }

    public void setNext(Node<K, V> next) {
        if (this.next != null && next == null) {
            System.out.println(key + " 要被设置空next");
        }
        this.next = next;
    }

    public void setNextToEnd(Node<K, V> next) {
        this.next = next;
    }

    public boolean isUpdated() {
        return isUpdated;
    }

    public void setUpdated(boolean updated) {
        isUpdated = updated;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
        this.setUpdated(true);
    }
}

