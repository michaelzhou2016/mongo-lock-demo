package com.example.service;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

@Slf4j
@Component
@RequiredArgsConstructor
public class ResumeTokenStore {

    private static final String COLLECTION = "ChangeStreamResumeToken";
    private final MongoTemplate mongoTemplate;

    public void save(String name, BsonDocument token) {
        mongoTemplate.getCollection(COLLECTION).replaceOne(
                new Document("_id", name),
                new Document("_id", name).append("token", token.toJson()),
                new ReplaceOptions().upsert(true));
    }

    /**
     * 保存 token。ts 单调递增:已存在更新的 token 时不会被旧值覆盖。
     * 原理:filter 不匹配时 upsert 会触发 _id 冲突 → 忽略。
     */
    public void save(String name, BsonDocument token, BsonTimestamp clusterTime, String owner) {
        long ts = clusterTime == null ? 0L : clusterTime.getValue();
        Bson filter = and(eq("_id", name), lte("ts", ts));
        Bson update = combine(
                set("token", token.toJson()),
                set("ts", ts),
                set("updatedAt", LocalDateTime.now()),
                set("updatedBy", owner));
        try {
            mongoTemplate.getCollection(COLLECTION).updateOne(filter, update, new UpdateOptions().upsert(true));
        } catch (MongoWriteException e) {
            if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                log.warn("Stale resume token ignored for [{}], a newer one exists", name);
                return;
            }
            throw e;
        }
    }

    public BsonDocument load(String name) {
        Document doc = mongoTemplate.getCollection(COLLECTION)
                .find(eq("_id", name)).first();
        return (doc == null || !doc.containsKey("token")) ? null : BsonDocument.parse(doc.getString("token"));
    }

    public void clear(String name) {
        mongoTemplate.getCollection(COLLECTION).deleteOne(eq("_id", name));
    }
}
