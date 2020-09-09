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

package io.hetu.core.plugin.carbondata;

public class CarbondataConstants
{
    public static final String SegmentNoRowCountMapping = "hetu.carbondata.states.segmentNoRowCountMapping";
    public static final String CarbonTable = "hetu.carbondata.states.carbonTable";
    public static final String EncodedLoadModel = "hetu.carbondata.states.encodedLoadModel";
    public static final String NewSegmentId = "hetu.carbondata.states.newSegmentId";
    public static final String TxnBeginTimeStamp = "hetu.carbondata.states.transactionbegintimestamp";
    public static final String TaskId = "mapred.task.id";
    public static final String MajorCompaction = "MAJOR";
    public static final String MinorCompaction = "MINOR";

    private CarbondataConstants()
    {
    }
}
