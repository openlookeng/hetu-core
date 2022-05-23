/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.redis;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.prestosql.plugin.redis.decoder.RedisDecoderModule;
import io.prestosql.plugin.redis.description.RedisTableDescription;
import io.prestosql.plugin.redis.description.RedisTableDescriptionSupplier;
import io.prestosql.plugin.redis.handle.RedisTableHandle;
import io.prestosql.plugin.redis.record.RedisRecordSetProvider;
import io.prestosql.plugin.redis.split.RedisSplitManager;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public class RedisModule
        implements Module
{
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier;

    public RedisModule(
            final TypeManager typeManager,
            final Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier,
            final NodeManager nodeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableDescriptionSupplier = tableDescriptionSupplier;
        this.nodeManager = requireNonNull(nodeManager, "nodeManger is null");
    }

    @Override
    public void configure(final Binder binder)
    {
        configBinder(binder).bindConfig(RedisConfig.class);
        if (tableDescriptionSupplier.isPresent()) {
            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, RedisTableDescription>>>() { }).toInstance(tableDescriptionSupplier.get());
        }
        else {
            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, RedisTableDescription>>>() { })
                    .to(RedisTableDescriptionSupplier.class)
                    .in(Scopes.SINGLETON);
        }
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(RedisConnector.class).in(Scopes.SINGLETON);
        binder.bind(RedisMetadata.class).in(Scopes.SINGLETON);
        binder.bind(RedisSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(RedisRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(RedisJedisManager.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(RedisTableHandle.class));
        jsonCodecBinder(binder).bindJsonCodec(RedisTableDescription.class);
        binder.install(new RedisDecoderModule());
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(final TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(final String value, final DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }
}
