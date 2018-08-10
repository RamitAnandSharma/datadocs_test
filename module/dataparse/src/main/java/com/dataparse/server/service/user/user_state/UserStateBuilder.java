package com.dataparse.server.service.user.user_state;

import com.dataparse.server.service.user.user_state.state.*;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserStateBuilder {

    public UserState create(Long userId) {
        UserState userState = new UserState(userId);

        userState.setDocsSectionColumns(generateDocsSectionColumnsSettings());
        userState.setSourcesSectionColumns(generateSourcesSectionColumnsSettings());
        userState.setRecentDocsSectionColumns(generateRecentSectionColumnsSettings());
        userState.setRecentSourcesSectionColumns(generateRecentSectionColumnsSettings());

        return userState;
    }

    // todo refactoring

    private List<ColumnsSectionSettings> generateDocsSectionColumnsSettings() {
        List<ColumnsSectionSettings> colSectionSettings = Lists.newArrayList(
                new ColumnsSectionSettings(),
                new ColumnsSectionSettings(),
                new ColumnsSectionSettings()
        );

        colSectionSettings.get(1).setSortSettings(new SortSettings(SortDirection.DESC));

        return colSectionSettings;
    }

    private List<ColumnsSectionSettings> generateSourcesSectionColumnsSettings() {
        List<ColumnsSectionSettings> colSectionSettings = Lists.newArrayList(
                new ColumnsSectionSettings(),
                new ColumnsSectionSettings(),
                new ColumnsSectionSettings()
        );
        colSectionSettings.get(2).setSortSettings(new SortSettings(SortDirection.DESC));
        return colSectionSettings;
    }

    private List<ColumnsSectionSettings> generateRecentSectionColumnsSettings() {
        List<ColumnsSectionSettings> colSectionSettings = Lists.newArrayList(
                new ColumnsSectionSettings(),
                new ColumnsSectionSettings(),
                new ColumnsSectionSettings()
        );
        colSectionSettings.get(0).setSortSettings(new SortSettings(true));
        colSectionSettings.get(1).setSortSettings(new SortSettings(true));
        colSectionSettings.get(2).setSortSettings(new SortSettings(true));

        return colSectionSettings;
    }

}
