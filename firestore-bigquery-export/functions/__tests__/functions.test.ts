import mockedEnv from "mocked-env";

const recordMock = jest.fn();
const serializeMock = jest.fn((d) => d);

jest.mock("firebase-functions/v2", () => ({
  __esModule: true,
  setGlobalOptions: jest.fn(),
}));

jest.mock("firebase-functions/v2/tasks", () => ({
  __esModule: true,
  onTaskDispatched: (handler) => handler,
}));

jest.mock("firebase-functions/v2/firestore", () => ({
  __esModule: true,
  onDocumentWritten: (_opts, handler) => handler,
}));

jest.mock("../src/change-tracker", () => ({
  ChangeType: { CREATE: 0, UPDATE: 1, DELETE: 2 },
  FirestoreBigQueryEventHistoryTracker: jest.fn(() => ({
    record: recordMock,
    serializeData: serializeMock,
  })),
}));

const enqueueMock = jest.fn();
jest.mock("firebase-admin/functions", () => ({
  getFunctions: () => ({
    taskQueue: jest.fn(() => ({
      enqueue: enqueueMock,
    })),
  }),
}));

jest.mock("../src/logs", () => ({
  logEventAction: jest.fn(),
  logFailedEventAction: jest.fn(),
  start: jest.fn(),
  init: jest.fn(),
  error: jest.fn(),
  complete: jest.fn(),
}));

jest.mock("../src/events", () => ({
  setupEventChannel: jest.fn(),
  recordStartEvent: jest.fn(),
  recordSuccessEvent: jest.fn(),
  recordErrorEvent: jest.fn(),
}));

const defaultEnvironment = {
  PROJECT_ID: "fake-project",
  DATASET_ID: "my_ds_id",
  TABLE_ID: "my_id",
  COLLECTION_PATH: "example/{docId}",
  DATABASE_ID: "(default)",
  LOCATION: "us-central1",
};

describe("v2 functions", () => {
  let restoreEnv: () => void;

  beforeEach(() => {
    jest.resetModules();
    recordMock.mockClear();
    serializeMock.mockClear();
    enqueueMock.mockClear();
    restoreEnv = mockedEnv(defaultEnvironment);
  });

  afterEach(() => restoreEnv());

  test("exports are functions", () => {
    const exported = require("../src");
    expect(typeof exported.fsexportbigquery).toBe("function");
    expect(typeof exported.syncBigQuery).toBe("function");
  });

  test("fsexportbigquery handles create event", async () => {
    const { fsexportbigquery } = require("../src");

    const event = {
      data: {
        before: { exists: false, id: "doc1", data: () => undefined },
        after: {
          exists: true,
          id: "doc1",
          data: () => ({ foo: "bar" }),
          ref: { path: "example/doc1" },
        },
      },
      params: { docId: "doc1" },
      id: "evt-1",
      time: "2020-01-01T00:00:00.000Z",
    };

    await fsexportbigquery(event);

    expect(recordMock).toHaveBeenCalledTimes(1);
    const recordedEvent = recordMock.mock.calls[0][0][0];
    expect(recordedEvent.documentId).toBe("doc1");
    expect(recordedEvent.operation).toBe(0); // ChangeType.CREATE
  });

  test("syncBigQuery writes event", async () => {
    const { syncBigQuery } = require("../src");

    const event = {
      data: {
        context: {
          resource: {
            name: "projects/fake/databases/(default)/documents/example/doc2",
          },
          eventId: "evt-2",
          timestamp: "2020-01-01T00:00:00.000Z",
          params: { docId: "doc2" },
        },
        changeType: 0,
        documentId: "doc2",
        data: { foo: "bar" },
        oldData: undefined,
      },
      id: "task-1",
      time: "2020-01-01T00:00:01.000Z",
    };

    await syncBigQuery(event);

    expect(recordMock).toHaveBeenCalledTimes(1);
    const recordedEvent = recordMock.mock.calls[0][0][0];
    expect(recordedEvent.documentId).toBe("doc2");
    expect(recordedEvent.operation).toBe(0); // ChangeType.CREATE
  });
});
