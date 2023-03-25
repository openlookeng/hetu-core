package io.hetu.core.filesystem.db;

import java.nio.file.Path;

class DbCatalog {
    private String catalogName;

    private String metadata;

    private Path metadataDirPath;

    private String properties;

    private Path propertiesPath;

    public DbCatalog() {
    }

    public DbCatalog(String catalogName, String metadata, String properties) {
        this.catalogName = catalogName;
        this.metadata = metadata;
        this.properties = properties;
    }

    public DbCatalog(String catalogName, String metadata, Path metadataDirPath, String properties, Path propertiesPath) {
        this.catalogName = catalogName;
        this.metadata = metadata;
        this.metadataDirPath = metadataDirPath;
        this.properties = properties;
        this.propertiesPath = propertiesPath;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public Path getMetadataDirPath() {
        return metadataDirPath;
    }

    public void setMetadataDirPath(Path metadataDirPath) {
        this.metadataDirPath = metadataDirPath;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public Path getPropertiesPath() {
        return propertiesPath;
    }

    public void setPropertiesPath(Path propertiesPath) {
        this.propertiesPath = propertiesPath;
    }
}
