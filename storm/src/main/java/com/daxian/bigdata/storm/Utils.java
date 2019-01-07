package com.daxian.bigdata.storm;

import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Utils {

    public static Set<Integer> splitNumber(int number){

        Set result = new HashSet();

        while (number > 0){
            if(!result.contains(number % 10)){
                result.add(number % 10);
            }
            number = number / 10;
        }

        return result;
    }

    /**
     * 判断x，y 和 count包含的数字，有没有重复
     * @param x
     * @param y
     * @param count
     * @return
     */
    public static boolean intersectionCheck(int x,int y,int count){

        boolean result = false;

        //等号左侧x+y
        Set leftSet = new HashSet();
        leftSet.addAll(Utils.splitNumber(x));
        leftSet.addAll(Utils.splitNumber(y));

        //等号右侧结果
        Set rightSet = Utils.splitNumber(count);

        //左右结果的交集
        Set intersection = new HashSet();
        intersection.addAll(leftSet);
        intersection.retainAll(rightSet);

        //没有交集说明满足条件
        if(intersection.size()==0){
            return true;
        }

        return result;
    }

}
