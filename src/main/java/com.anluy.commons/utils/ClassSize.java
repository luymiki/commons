/*
 * Copyright 2017 com.anluy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.anluy.commons.utils;

/**
 * Created by liqiang on 15/12/12.
 */
public class ClassSize {

    public static  final int DefaultRecordHead;
    public static  final int ColumnHead;

    //objectHead的大小
    public static final int REFERENCE;
    public static final int OBJECT;
    public static final int ARRAY;
    public static final int ARRAYLIST;
    static {
        //only 64位
        REFERENCE = 8;

        OBJECT = 2 * REFERENCE;

        ARRAY = align(3 * REFERENCE);

        // 16+8+24+16
        ARRAYLIST = align(OBJECT + align(REFERENCE) + align(ARRAY) +
                (2 * Long.SIZE / Byte.SIZE));
        // 8+64+8
        DefaultRecordHead = align(align(REFERENCE) + ClassSize.ARRAYLIST + 2 * Integer.SIZE / Byte.SIZE);
        //16+4
        ColumnHead = align(2 * REFERENCE + Integer.SIZE / Byte.SIZE);
    }

    public static int align(int num) {
        return (int)(align((long)num));
    }

    public static long align(long num) {
        //The 7 comes from that the alignSize is 8 which is the number of bytes
        //stored and sent together
        return  ((num + 7) >> 3) << 3;
    }
}
