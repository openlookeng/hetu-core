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
package io.prestosql.protocol;

public interface Codec<T>
{
    /**
     * Serialize the given instance of type T to a byte array.
     *
     * @param instance The instance to serialize.
     * @return a byte[] that holds the bytes for the serialized instance.
     */
    byte[] toBytes(T instance);
}
