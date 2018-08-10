package com.dataparse.server.service.user.user_state.event.docs_section_columns;


import com.dataparse.server.service.user.user_state.event.*;
import com.dataparse.server.service.user.user_state.state.*;
import lombok.*;

@Data
public class DocsSectionColumnsSelectedChangeEvent extends UserStateChangeEvent {

    private Integer columnIndex;
    private Integer selected;
    private SortDirection newDirection;

    @Override
    public void apply(final UserState state) {
        state.getDocsSectionColumns().get(columnIndex).setSelected(selected);

        if (newDirection != null) {
            for (int i = 0; i < state.getDocsSectionColumns().size(); ++i) {

                if (i == columnIndex) {
                    state.getDocsSectionColumns().get(i).getSortSettings().setDirection(newDirection);
                } else {
                    state.getDocsSectionColumns().get(i).getSortSettings().setDirection(null);
                }
            }
        }
    }
}
