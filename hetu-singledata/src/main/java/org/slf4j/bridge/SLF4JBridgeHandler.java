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

package org.slf4j.bridge;

/**
 * This class is only for exclude jul-to-slf4j dependencies.
 * ShardingSphere depends on jul-to-slf4j. It is conflicts with slf4j-jdk14 on which openLookeng depends. As a result,
 * logs are cyclically dependent. So, We made this empty SLFJBridgeHandle to exclude jul-to-slf4j dependencies.
 */
public class SLF4JBridgeHandler
{
    public static void install() {}

    public static void removeHandlersForRootLogger() {}

    private SLF4JBridgeHandler() {}
}
