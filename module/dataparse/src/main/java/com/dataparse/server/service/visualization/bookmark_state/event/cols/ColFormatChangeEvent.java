package com.dataparse.server.service.visualization.bookmark_state.event.cols;

import com.dataparse.server.service.visualization.bookmark_state.event.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
public class ColFormatChangeEvent extends BookmarkVizStateChangeEvent {

    private boolean rememberLastSelectedCurrency;
    private String field;
    private ColFormat format;

    @Override
    public void apply(final BookmarkState state) {
        Col col = state.getColumnList().stream().filter(c -> c.getField().equals(field)).findFirst().orElse(null);
        if(col != null) {
            col.getSettings().setFormat(format);
            if(rememberLastSelectedCurrency) {
                state.setLastSelectedCurrency(format.getCurrency());
            }
        }
    }
}
