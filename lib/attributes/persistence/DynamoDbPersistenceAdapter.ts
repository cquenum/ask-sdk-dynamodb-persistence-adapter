/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import {
    createAskSdkError,
    PersistenceAdapter
} from 'ask-sdk-core';
import { RequestEnvelope } from 'ask-sdk-model';
import { CreateTableCommand, CreateTableInput, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DeleteCommand, DeleteCommandInput, DynamoDBDocumentClient, GetCommand, GetCommandInput, GetCommandOutput, PutCommand, PutCommandInput } from '@aws-sdk/lib-dynamodb';
import {
    PartitionKeyGenerator,
    PartitionKeyGenerators
} from './PartitionKeyGenerators';

export class DynamoDbPersistenceAdapter implements PersistenceAdapter {
    protected tableName: string;
    protected partitionKeyName: string;
    protected attributesName: string;
    protected createTable: boolean;
    protected dynamoDBClient: DynamoDBClient;
    protected partitionKeyGenerator: PartitionKeyGenerator;
    protected dynamoDBDocumentClient: DynamoDBDocumentClient;

    constructor(config: {
        tableName: string,
        partitionKeyName?: string,
        attributesName?: string,
        createTable?: boolean,
        dynamoDBClient?: DynamoDBClient,
        partitionKeyGenerator?: PartitionKeyGenerator;
    }) {
        this.tableName = config.tableName;
        this.partitionKeyName = config.partitionKeyName ? config.partitionKeyName : 'id';
        this.attributesName = config.attributesName ? config.attributesName : 'attributes';
        this.createTable = config.createTable === true;
        this.dynamoDBClient = config.dynamoDBClient ? config.dynamoDBClient : new DynamoDBClient({ apiVersion: 'latest' });
        this.partitionKeyGenerator = config.partitionKeyGenerator ? config.partitionKeyGenerator : PartitionKeyGenerators.userId;
        this.dynamoDBDocumentClient = DynamoDBDocumentClient.from(this.dynamoDBClient, {
            marshallOptions: { convertEmptyValues: true },
            unmarshallOptions: { wrapNumbers: false },
        });

        if (this.createTable) {
            const createTableParams: CreateTableInput = {
                TableName: this.tableName,
                AttributeDefinitions: [{
                    AttributeName: this.partitionKeyName,
                    AttributeType: 'S',
                }],
                KeySchema: [{
                    AttributeName: this.partitionKeyName,
                    KeyType: 'HASH',
                }],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 5,
                    WriteCapacityUnits: 5,
                }
            };

            this.dynamoDBClient.send(new CreateTableCommand(createTableParams))
                .catch(error => {
                    if (error.code !== 'ResourceInUseException') {
                        throw createAskSdkError(
                            this.constructor.name,
                            `Could not create table (${this.tableName}): ${error.message}`,
                        );
                    }
                });
        }
    }

    /**
     * Retrieves persistence attributes from AWS DynamoDB.
     * @param {RequestEnvelope} requestEnvelope Request envelope used to generate partition key.
     * @returns {Promise<Object.<string, any>>}
     */
    public async getAttributes(requestEnvelope: RequestEnvelope): Promise<{ [key: string]: any; }> {
        const attributesId = this.partitionKeyGenerator(requestEnvelope);

        const getParams: GetCommandInput = {
            Key: {
                [this.partitionKeyName]: attributesId
            },
            TableName: this.tableName,
            ConsistentRead: true,
        };

        let data: GetCommandOutput;
        try {
            data = await this.dynamoDBDocumentClient.send(new GetCommand(getParams));
        } catch (error) {
            throw createAskSdkError(
                this.constructor.name,
                `Could not read item (${attributesId}) from table (${getParams.TableName}): ${error.message}`,
            );
        }

        if (data && data.Item) {
            const result = data.Item[this.attributesName] || {};
            return result;
        }

        return {};
    }

    /**
     * Saves persistence attributes to AWS DynamoDB.
     * @param {RequestEnvelope} requestEnvelope Request envelope used to generate partition key.
     * @param {Object.<string, any>} attributes Attributes to be saved to DynamoDB.
     * @return {Promise<void>}
     */
    public async saveAttributes(requestEnvelope: RequestEnvelope, attributes: { [key: string]: any; }): Promise<void> {
        const attributesId = this.partitionKeyGenerator(requestEnvelope);

        const putParams: PutCommandInput = {
            Item: {
                [this.partitionKeyName]: attributesId,
                [this.attributesName]: attributes,
            },
            TableName: this.tableName,
        };

        try {
            await this.dynamoDBDocumentClient.send(new PutCommand(putParams));
        } catch (err) {
            throw createAskSdkError(
                this.constructor.name,
                `Could not save item (${attributesId}) to table (${putParams.TableName}): ${err.message}`,
            );
        }
    }

    /**
     * Delete persistence attributes from AWS DynamoDB.
     * @param {RequestEnvelope} requestEnvelope Request envelope used to generate partition key.
     * @return {Promise<void>}
     */
    public async deleteAttributes(requestEnvelope: RequestEnvelope): Promise<void> {
        const attributesId = this.partitionKeyGenerator(requestEnvelope);

        const deleteParams: DeleteCommandInput = {
            Key: {
                [this.partitionKeyName]: attributesId,
            },
            TableName: this.tableName,
        };

        try {
            await this.dynamoDBDocumentClient.send(new DeleteCommand(deleteParams));
        } catch (error) {
            throw createAskSdkError(
                this.constructor.name,
                `Could not delete item (${attributesId}) from table (${deleteParams.TableName}): ${error.message}`,
            );
        }
    }
}
