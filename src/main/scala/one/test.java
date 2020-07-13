package one;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BinaryOperator;

public class test {
    public static void main(String[] args){
//        PriorityQueue<Integer> pq = new PriorityQueue();
//        System.out.println(pq.peek());
        String s = "a    d c e";
        s.substring(1,1);
        String[] sa = s.split(" ");
        for(String str: sa)
            System.out.println(str + str.equals(" "));
    }
}
