package cromwell.backend.impl.vk;


public enum VkDiskType {
    LOCAL("LOCAL", "ssd"),
    SSD("SSD", "ssd"),
    HDD("HDD", "sas"),
    SATA("SATA", "sata"),
    SAS("SAS", "sas");

    public final String diskTypeName;
    public final String hwsTypeName;

    VkDiskType(final String diskTypeName, final String hwsTypeName) {
        this.diskTypeName = diskTypeName;
        this.hwsTypeName = hwsTypeName;
    }
}