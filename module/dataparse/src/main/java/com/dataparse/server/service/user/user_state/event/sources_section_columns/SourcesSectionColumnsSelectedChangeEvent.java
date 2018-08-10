package com.dataparse.server.service.user.user_state.event.sources_section_columns;


import com.dataparse.server.service.user.user_state.event.*;
import com.dataparse.server.service.user.user_state.state.*;
import lombok.*;

@Data
public class SourcesSectionColumnsSelectedChangeEvent extends UserStateChangeEvent {

    private Integer columnIndex;
    private Integer selected;
    private SortDirection newDirection;

    @Override
    public void apply(final UserState state) {        
        state.getSourcesSectionColumns().get(columnIndex).setSelected(selected);

        if (newDirection != null) {
            for (int i = 0; i < state.getSourcesSectionColumns().size(); ++i) {

                if (i == columnIndex) {
                    state.getSourcesSectionColumns().get(i).getSortSettings().setDirection(newDirection);
                } else {
                    state.getSourcesSectionColumns().get(i).getSortSettings().setDirection(null);
                }
            }
        }
    }
}
