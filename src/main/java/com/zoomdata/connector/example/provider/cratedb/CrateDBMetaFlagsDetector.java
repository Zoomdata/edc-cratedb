/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb;

import com.querydsl.sql.SQLTemplates;
import com.zoomdata.gen.edc.request.CollectionInfo;
import com.zoomdata.gen.edc.types.FieldMetadata;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;

import static com.zoomdata.connector.example.common.utils.FieldMetaFlag.PLAYABLE;
import static com.zoomdata.connector.example.common.utils.FieldMetaFlag.addFlags;
import static com.zoomdata.connector.example.framework.common.PropertiesExtractor.tableName;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.left;
import static org.apache.commons.lang3.StringUtils.substringBetween;

@Slf4j
public class CrateDBMetaFlagsDetector {

    public static final String SHOW_CREATE_TABLE = "show create table ";

    private final SQLTemplates sqlTemplates;

    public CrateDBMetaFlagsDetector(SQLTemplates sqlTemplates) {
        this.sqlTemplates = sqlTemplates;
    }

    public void populateMetaFlags(Connection connection, CollectionInfo collectionInfo, List<FieldMetadata> metadata) {
        getCreateTableStatement(connection, collectionInfo)
                .map(this::extractPartitionedFields)
                .ifPresent(partitionedFields -> assignMetaFlagsToFields(metadata, partitionedFields));
    }

    private Optional<String> getCreateTableStatement(Connection connection, CollectionInfo collectionInfo) {
        try (Statement ps = connection.createStatement();
             ResultSet rs = ps.executeQuery(showStatement(collectionInfo))) {
            return Optional.ofNullable(extractResultSet(rs));
        } catch (SQLException e) {
            log.warn("Failed to obtain CREATE TABLE statement for the schema [{}] and collection [{}]",
                    collectionInfo.getSchema(), collectionInfo.getCollection(), e);
            return Optional.empty();
        }
    }

    private String showStatement(CollectionInfo collectionInfo) {
        return SHOW_CREATE_TABLE + tableName(collectionInfo, sqlTemplates);
    }

    private String extractResultSet(ResultSet resultSet) throws SQLException {
        StringJoiner joiner = new StringJoiner(" ");
        while(resultSet.next()) {
            joiner.add(resultSet.getString(1));
        }
        return joiner.toString();
    }

    /**
     * Extracts names of partitioned fields from the PARTITIONED BY clause of a create statement. For example:
     * <pre>
     * PARTITIONED BY (
     *      first_partition_key INT COMMENT 'The comment',
     *      another_partition_key INT
     * )
     * </pre>
     */
    private Set<String> extractPartitionedFields(String createStatement) {
        String partitionedByClause = ofNullable(substringBetween(createStatement, "PARTITIONED BY (", ")")).orElse(EMPTY);
        return of(partitionedByClause.split(","))
                .map(String::trim)
                .map(this::extractColumnName)
                .collect(toSet());
    }

    private String extractColumnName(String columnDefinition) {
        String quoteCharacter = getQuoteIdentifier();
        return columnDefinition.split("\\s+")[0].replaceAll("^" + quoteCharacter + "|" + quoteCharacter + "$", "");
    }

    private String getQuoteIdentifier() {
        // QueryDSL does not provide a simple way to get quote string so instead we'll put quotes around an identifier
        String quoteIdentifier = left(sqlTemplates.quoteIdentifier(" "), 1);
        return ofNullable(quoteIdentifier).orElse(EMPTY);
    }

    private void assignMetaFlagsToFields(List<FieldMetadata> metadata, Set<String> partitionedFields) {
        Predicate<FieldMetadata> partitionedField = fieldMetadata -> partitionedFields.contains(fieldMetadata.getName().toLowerCase());
        metadata.stream()
                .filter(partitionedField)
                .forEach(fieldMetadata -> addFlags(fieldMetadata, PLAYABLE));
    }

}
