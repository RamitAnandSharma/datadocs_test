package com.dataparse.server.service.flow.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinCondition {

    private String leftKey;
    private JoinFn fn;
    private String rightKey;

    public static JoinCondition of(String leftKey, JoinFn fn, String rightKey){
        return new JoinCondition(leftKey, fn, rightKey);
    }

    public static JoinCondition invert(JoinCondition condition){
        return new JoinCondition(condition.rightKey, condition.fn, condition.leftKey);
    }

    public static List<JoinCondition> invert(List<JoinCondition> conditions){
        return conditions.stream().map(c -> invert(c)).collect(Collectors.toList());
    }

    public boolean isMet(Map<String, Object> leftRow, Map<String, Object> rightRow){
        Object leftValue = leftRow.get(this.getLeftKey());
        Object rightValue = rightRow.get(this.getRightKey());
        return this.getFn().execute(leftValue, rightValue);
    }

    public static boolean isMetAll(Map<String, Object> leftRow, Map<String, Object> rightRow, List<JoinCondition> conditions){
        for(JoinCondition condition : conditions){
            if(!condition.isMet(leftRow, rightRow)){
                return false;
            }
        }
        return true;
    }
}
