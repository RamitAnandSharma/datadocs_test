package com.dataparse.server.service.flow;


import com.dataparse.server.service.flow.node.*;

import java.util.*;

public class FlowTraverse {

    public static List<Node> getBfsOrderedList(Node root){
        List<Node> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Queue<Node> q = new ArrayDeque<>();
        q.add(root);
        result.add(root);
        while(!q.isEmpty()){
            Node i = q.remove();
            for(Object j : i.getChildren()){
                Node n = (Node) j;
                if(!seen.contains(n.getId())){
                    q.add(i);
                    seen.add(n.getId());
                    result.add(n);
                }
            }
        }
        return result;
    }

}
