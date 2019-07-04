/**
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dariobalinzo.schema;

import com.github.dariobalinzo.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.simple.JSONValue;

public class StructConverter {

    public static Struct convertElasticDocument2AvroStruct(Map<String, Object> doc, Schema schema,
                                                           List<String> whitelistFields, List<String> castStringFields) {

        Struct struct = new Struct(schema);
        convertDocumentStruct("",doc, struct,schema, whitelistFields, castStringFields);
        return struct;

    }


    private static void convertDocumentStruct(String prefixName, Map<String, Object> doc, Struct struct, Schema schema,
                                              List<String> whitelistFields, List<String> castStringFields) {

        doc.keySet().forEach(
                k -> {
                    if (whitelistFields.size() > 0 && !whitelistFields.contains(k)) {
                        return;
                    }
                    Object v = doc.get(k);
                    if (v == null){
                        return;
                    } else if ( castStringFields.size() > 0 && castStringFields.contains(k) ) {
                        String jsonValue = JSONValue.toJSONString(v);
                        struct.put(Utils.filterAvroName(k), jsonValue);
                    } else if (v instanceof String  ) {
                        struct.put(Utils.filterAvroName(k), v);
                    } else if (v instanceof Boolean  ) {
                        struct.put(Utils.filterAvroName(k), v);
                    } else if (v instanceof Integer || v instanceof Long) {
                        struct.put(Utils.filterAvroName(k), v);
                    } else if (v instanceof Double || v instanceof Float) {
                        struct.put(Utils.filterAvroName(k), v);
                    } else if (v instanceof List) {

                        if (!((List) v).isEmpty()) {
                            //assuming that every item of the list has the same schema
                            Object item = ((List) v).get(0);
                            struct.put(Utils.filterAvroName(k),new ArrayList<>());
                            if (item == null) {
                                return;
                            } else if (item instanceof String) {
                                struct.getArray(Utils.filterAvroName(k)).addAll((List) v);
                            } else if (item instanceof Boolean) {
                                struct.getArray(Utils.filterAvroName(k)).addAll((List) v);
                            } else if (item instanceof Integer || item instanceof Long) {
                                struct.getArray(Utils.filterAvroName(k)).addAll((List) v);
                            } else if (item instanceof Double || item instanceof Float) {
                                struct.getArray(Utils.filterAvroName(k)).addAll((List) v);
                            } else if (item instanceof Map) {

                                List<Struct> array = (List<Struct>) ((List) v)
                                        .stream()
                                        .map(i -> {
                                            Struct nestedStruct = new Struct(schema.field(Utils.filterAvroName(prefixName,k)).schema().valueSchema());
                                            convertDocumentStruct(
                                                    Utils.filterAvroName(prefixName,k)+".",
                                                    (Map<String, Object>) i, nestedStruct, schema.field(Utils.filterAvroName(k)).schema().valueSchema(),
                                                    whitelistFields,
                                                    castStringFields);
                                            return nestedStruct;
                                        }).collect(Collectors.toCollection(ArrayList::new));
                                struct.put(Utils.filterAvroName(k),array );
                            } else {
                                throw new RuntimeException("error in converting list: type not supported");
                            }

                        }

                    } else if (v instanceof Map) {

                        Struct nestedStruct = new Struct(schema.field(Utils.filterAvroName(k)).schema());
                        convertDocumentStruct(
                                Utils.filterAvroName(prefixName,k)+".",
                                (Map<String, Object>) v,
                                nestedStruct,
                                schema.field(Utils.filterAvroName(k)).schema(),
                                whitelistFields,
                                castStringFields
                        );
                        struct.put(Utils.filterAvroName(k),nestedStruct);

                    } else {
                        String message = String.format("key: [%s]; v: [%s], v_type: [%s] doc: [%s]",
                            k, v, v.getClass().getName(), doc);
                        throw new RuntimeException("type not supported" + message);
                    }
                }
        );

    }


}
