/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.sql.builder.functioncall.functions;

import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;

/**
 * Function Call Rewrite support convert Hetu internal or build-in function call convert to datasource’s function call
 * through rewrite the Hetu inner api to datasource‘s sql grammar
 *
 * @since 2019-09-10
 */
public interface FunctionCallRewriter
{
    /**
     * functionCall rewrite interface api the package of SqlQueryWriter's function call methods args
     *
     * @param functionCallArgsPackage the package of SqlQueryWriter's function call methods args
     * @return sql statement of function call in datasource’s grammar
     */
    String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage);
}
