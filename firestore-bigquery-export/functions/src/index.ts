/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import config from "./config";
import { setGlobalOptions } from "firebase-functions/v2";
import { onDocumentWritten } from "firebase-functions/v2/firestore";
import { onTaskDispatched } from "firebase-functions/v2/tasks";
const taskDispatcher =
  typeof onTaskDispatched === "function"
    ? onTaskDispatched
    : (handler: any) => handler;
const firestoreWriter =
  typeof onDocumentWritten === "function"
    ? onDocumentWritten
    : (_opts: any, handler: any) => handler;
import { logger } from "firebase-functions";
import * as admin from "firebase-admin";
import { getExtensions } from "firebase-admin/extensions";
import { getFunctions } from "firebase-admin/functions";
import {
  ChangeType,
  FirestoreBigQueryEventHistoryTracker,
  FirestoreDocumentChangeEvent,
} from "./change-tracker";

import * as logs from "./logs";
import * as events from "./events";
import { getChangeType, getDocumentId } from "./util";

// Set default region for all functions when available (handles test mocks).
if (typeof setGlobalOptions === "function") {
  setGlobalOptions({ region: config.location });
}

// Configuration for the Firestore Event History Tracker.
const eventTrackerConfig = {
  tableId: config.tableId,
  datasetId: config.datasetId,
  datasetLocation: config.datasetLocation,
  backupTableId: config.backupCollectionId,
  transformFunction: config.transformFunction,
  timePartitioning: config.timePartitioning,
  timePartitioningField: config.timePartitioningField,
  timePartitioningFieldType: config.timePartitioningFieldType,
  timePartitioningFirestoreField: config.timePartitioningFirestoreField,
  // Database related configurations
  databaseId: config.databaseId,
  clustering: config.clustering,
  wildcardIds: config.wildcardIds,
  bqProjectId: config.bqProjectId,
  // Optional configurations
  useNewSnapshotQuerySyntax: config.useNewSnapshotQuerySyntax,
  skipInit: true,
  kmsKeyName: config.kmsKeyName,
};

// Initialize the Firestore Event History Tracker with the given configuration.
const eventTracker: FirestoreBigQueryEventHistoryTracker =
  new FirestoreBigQueryEventHistoryTracker(eventTrackerConfig);

// Initialize logging.
logs.init();

/** Initialize Firebase Admin SDK if not already initialized */
if (admin.apps.length === 0) {
  admin.initializeApp();
}

// Setup the event channel for EventArc.
events.setupEventChannel();

/**
 * Cloud Function to handle enqueued tasks to synchronize Firestore changes to BigQuery.
 */
export const syncBigQuery = taskDispatcher(async (event) => {
  const { context, changeType, documentId, data, oldData } = event.data || {};
  const documentName =
    context?.resource?.name ||
    `projects/${process.env.GCLOUD_PROJECT}/databases/${config.databaseId}/documents/${documentId}`;
  const eventId = context?.eventId || event.id;
  const timestamp = context?.timestamp || new Date().toISOString();
  const pathParams = config.wildcardIds ? context?.params : null;
  const operation = changeType;

  logs.logEventAction(
    "Firestore event received by onDispatch trigger",
    documentName,
    eventId,
    operation
  );

  try {
    await recordEventToBigQuery(changeType, documentId, data, oldData, {
      eventId,
      timestamp,
      resourceName: documentName,
      params: pathParams || undefined,
    });

    await events.recordSuccessEvent({
      subject: documentId,
      data: {
        timestamp,
        operation: changeType,
        documentName,
        documentId,
        pathParams,
        eventId,
        data,
        oldData,
      },
    });

    logs.complete();
  } catch (err) {
    logs.logFailedEventAction(
      "Failed to write event to BigQuery from onDispatch handler",
      documentName,
      eventId,
      operation,
      err as Error
    );

    throw err;
  }
});

/**
 * Cloud Function triggered on Firestore document changes to export data to BigQuery.
 */
export const fsexportbigquery = firestoreWriter(
  {
    document: config.collectionPath,
    database: config.databaseId,
  },
  async (event) => {
    // Start logging the function execution.
    logs.start();

    const change = event.data;

    // Determine the type of change (CREATE, UPDATE, DELETE).
    const changeType = getChangeType(change as any);
    const documentId = getDocumentId(change as any);

    // Check if the document is newly created or deleted.
    const isCreated = changeType === ChangeType.CREATE;
    const isDeleted = changeType === ChangeType.DELETE;

    // Get the new data (after change) and old data (before change).
    const data = isDeleted ? undefined : change?.after?.data();
    const oldData =
      isCreated || config.excludeOldData ? undefined : change?.before?.data();

    const docPath =
      change?.after?.ref?.path ?? change?.before?.ref?.path ?? documentId;
    const documentName = `projects/${process.env.GCLOUD_PROJECT}/databases/${config.databaseId}/documents/${docPath}`;
    const eventId = event.id;
    const operation = changeType;
    const timestamp = event.time;

    logs.logEventAction(
      "Firestore event received by onWrite trigger",
      documentName,
      eventId,
      operation
    );

    let serializedData: any;
    let serializedOldData: any;

    try {
      // Serialize the data before processing.
      serializedData = eventTracker.serializeData(data);
      serializedOldData = eventTracker.serializeData(oldData);
    } catch (err) {
      logs.logFailedEventAction(
        "Failed to serialize data",
        documentName,
        eventId,
        operation,
        err as Error
      );
      throw err;
    }

    try {
      // Record the start event for the change in EventArc, if configured.
      await events.recordStartEvent({
        documentId,
        changeType,
        before: { data: change?.before?.data() },
        after: { data: change?.after?.data() },
        context: documentName,
      });
    } catch (err) {
      logs.error(false, "Failed to record start event", err);
      throw err;
    }

    try {
      await recordEventToBigQuery(
        changeType,
        documentId,
        serializedData,
        serializedOldData,
        {
          eventId,
          timestamp,
          resourceName: documentName,
          params: config.wildcardIds ? event.params : undefined,
        }
      );
    } catch (err) {
      logger.warn(
        "Failed to write event to BigQuery Immediately. Will attempt to Enqueue to Cloud Tasks.",
        err
      );
      await attemptToEnqueue(
        err,
        {
          eventId,
          timestamp,
          resourceName: documentName,
          params: config.wildcardIds ? event.params : undefined,
        },
        changeType,
        documentId,
        serializedData,
        serializedOldData
      );
    }

    logs.complete();
  }
);

/**
 * Record the event to the Firestore Event History Tracker and BigQuery.
 *
 * @param changeType - The type of change (CREATE, UPDATE, DELETE).
 * @param documentId - The ID of the Firestore document.
 * @param serializedData - The serialized new data of the document.
 * @param serializedOldData - The serialized old data of the document.
 * @param context - The event context from Firestore.
 */
async function recordEventToBigQuery(
  changeType: ChangeType,
  documentId: string,
  serializedData: any,
  serializedOldData: any,
  context: {
    eventId: string;
    timestamp: string;
    resourceName: string;
    params?: Record<string, string> | null | undefined;
  }
) {
  const event: FirestoreDocumentChangeEvent = {
    timestamp: context.timestamp, // Cloud Firestore commit timestamp
    operation: changeType, // The type of operation performed
    documentName: context.resourceName, // The document name
    documentId, // The document ID
    pathParams: (config.wildcardIds ? context.params : null) as
      | FirestoreDocumentChangeEvent["pathParams"]
      | null, // Path parameters, if any
    eventId: context.eventId, // The event ID from Firestore
    data: serializedData, // Serialized new data
    oldData: serializedOldData, // Serialized old data
  };

  // Record the event in the Firestore Event History Tracker and BigQuery.
  await eventTracker.record([event]);
}

/**
 * Handle errors when enqueueing tasks to sync BigQuery.
 *
 * @param err - The error object.
 * @param context - The event context from Firestore.
 * @param changeType - The type of change (CREATE, UPDATE, DELETE).
 * @param documentId - The ID of the Firestore document.
 * @param serializedData - The serialized new data of the document.
 * @param serializedOldData - The serialized old data of the document.
 */
async function attemptToEnqueue(
  err: Error,
  context: {
    eventId: string;
    timestamp: string;
    resourceName: string;
    params?: Record<string, string> | null | undefined;
  },
  changeType: ChangeType,
  documentId: string,
  serializedData: any,
  serializedOldData: any
) {
  try {
    const queue = getFunctions().taskQueue(
      `locations/${config.location}/functions/syncBigQuery`,
      config.instanceId
    );

    let attempts = 0;
    const jitter = Math.random() * 100; // Adding jitter to avoid collision

    // Exponential backoff formula with a maximum of 5 + jitter seconds
    const backoff = (attempt: number) =>
      Math.min(Math.pow(2, attempt) * 100, 5000) + jitter;

    while (attempts < config.maxEnqueueAttempts) {
      if (attempts > 0) {
        // Wait before retrying to enqueue the task.
        await new Promise((resolve) => setTimeout(resolve, backoff(attempts)));
      }

      attempts++;
      try {
        // Attempt to enqueue the task to the queue.
        await queue.enqueue({
          context: {
            resource: { name: context.resourceName },
            eventId: context.eventId,
            timestamp: context.timestamp,
            params: context.params,
          },
          changeType,
          documentId,
          data: serializedData,
          oldData: serializedOldData,
        });
        break; // Break the loop if enqueuing is successful.
      } catch (enqueueErr) {
        // Throw the error if max attempts are reached.
        if (attempts === config.maxEnqueueAttempts) {
          throw enqueueErr;
        }
      }
    }
  } catch (enqueueErr) {
    // Prepare the event object for error logging.

    // Record the error event.
    await events.recordErrorEvent(enqueueErr as Error);

    const documentName = context.resourceName;
    const eventId = context.eventId;
    const operation = changeType;

    logs.logFailedEventAction(
      "Failed to enqueue event to Cloud Tasks from onWrite handler",
      documentName,
      eventId,
      operation,
      enqueueErr as Error
    );
  }
}

/**
 * Cloud Function to set up BigQuery sync by initializing the event tracker.
 */
export const setupBigQuerySync = taskDispatcher(async () => {
  /** Setup runtime environment */
  const runtime = getExtensions().runtime();

  // Initialize the BigQuery sync.
  await eventTracker.initialize();

  // Update the processing state.
  await runtime.setProcessingState(
    "PROCESSING_COMPLETE",
    "Sync setup completed"
  );
});

/**
 * Cloud Function to initialize BigQuery sync.
 */
export const initBigQuerySync = taskDispatcher(async () => {
  /** Setup runtime environment */
  const runtime = getExtensions().runtime();

  // Initialize the BigQuery sync.
  await eventTracker.initialize();

  // Update the processing state.
  await runtime.setProcessingState(
    "PROCESSING_COMPLETE",
    "Sync setup completed"
  );
  return;
});
