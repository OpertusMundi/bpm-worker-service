package eu.opertusmundi.bpm.worker.model;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import eu.opertusmundi.common.model.EnumAccountType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Builder
@Getter
public class DeleteAccountOperationContext {
    private Integer userId;

    private UUID userKey;

    private UUID userParentKey;

    private String userGeodataShard;

    private String userName;

    private EnumAccountType userType;

    private boolean accountDeleted;

    private boolean contractsDeleted;

    private boolean fileSystemDeleted;

    private Runnable extendLock;
    
    @Builder.Default
    private List<String> pid = new ArrayList<>();

    public boolean isFileSystemDeleted() {
        return this.accountDeleted || this.fileSystemDeleted;
    }
}