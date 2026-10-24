package com.ocx.book;

import java.util.AbstractList;

/**
 * 撮合热路径成交列表：只重置 size 的逻辑清空（元素由调用方池回收，无需置 null），
 * 避免 ArrayList.clear 对扩容后数组的全量置空（JFR 实测占撮合线程约 7%）。
 *
 * <p>仅实现撮合路径用到的操作（add/get/size/isEmpty/clear + 迭代）；清空后残留
 * 引用由下一次 add 覆盖，对象生命周期由调用方池管理。</p>
 */
public final class ReusableFillList extends AbstractList<MatchFill> {

    private MatchFill[] elements = new MatchFill[64];
    private int size;

    @Override
    public MatchFill get(int index) {
        return elements[index];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean add(MatchFill fill) {
        if (size == elements.length) {
            MatchFill[] grown = new MatchFill[elements.length << 1];
            System.arraycopy(elements, 0, grown, 0, size);
            elements = grown;
        }
        elements[size++] = fill;
        return true;
    }

    @Override
    public MatchFill remove(int index) {
        MatchFill removed = elements[index];
        int moved = size - index - 1;
        if (moved > 0) {
            System.arraycopy(elements, index + 1, elements, index, moved);
        }
        size--;
        return removed;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void clear() {
        size = 0; // 逻辑清空：不置 null
    }
}
