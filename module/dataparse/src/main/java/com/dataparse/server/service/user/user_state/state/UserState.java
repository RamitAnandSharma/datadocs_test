package com.dataparse.server.service.user.user_state.state;

import com.dataparse.server.config.AppConfig;
import com.dataparse.server.service.storage.StorageStrategyType;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.internal.util.SerializationHelper;
import org.mongojack.Id;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class UserState implements Serializable {

    public UserState(Long userId) {
        this.userId = userId;
    }

    @Id
    private Long userId;

    private Section activeSection = Section.MY_DATA;

    private List<ColumnsSectionSettings> docsSectionColumns = new ArrayList<>();
    private List<ColumnsSectionSettings> sourcesSectionColumns = new ArrayList<>();

    private List<ColumnsSectionSettings> recentDocsSectionColumns = new ArrayList<>();
    private List<ColumnsSectionSettings> recentSourcesSectionColumns = new ArrayList<>();

    private Integer selectedFolderId = null;

    private ShowTypesOptions showTypesOptions = new ShowTypesOptions();
    private StorageStrategyType storageStrategyType = AppConfig.getStorageStrategyType();

    public UserState copy(){
        return (UserState) SerializationHelper.clone(this);
    }

}
